package com.database.recovery;

import com.database.common.Buffer;
import com.database.common.iterator.BacktrackingIterable;
import com.database.common.iterator.BacktrackingIterator;
import com.database.common.iterator.ConcatBacktrackingIterator;
import com.database.common.iterator.IndexBacktrackingIterator;
import com.database.concurrency.DummyLockContext;
import com.database.io.DiskSpaceManager;
import com.database.io.PageException;
import com.database.memory.BufferManager;
import com.database.memory.Page;
import com.database.recovery.records.MasterLogRecord;

import java.util.*;

/**
 * The LogManager is responsible for interfacing with the log itself. The log is stored
 * on its own partition (partition 0). Since log pages are never deleted, the page number
 * is always increasing, so we assign LSNs as follow:
 * - page 1: [ LSN 10000, LSN 10040, LSN 10080, ...]
 * - page 2: [ LSN 20000, LSN 20030, LSN 20055, ...]
 * - page 3: [ LSN 30000, LSN 30047, LSN 30090, ...]
 * allowing for up to 10,000 log entries per page. The index (last 4 digits) is the offset
 * within the page where the log record starts. Log entries are not fixed width,
 * so backwards iteration is not as easy as forward iteration. Page 0 is reserved for the
 * master record, which only contains a few log entries: the master record, with LSN 0, followed
 * by an empty begin and end checkpoint record. The master record is the only record in the
 * entire log that may be rewritten.
 *
 * The LogManager is also responsible for writing pageLSNs onto pages and flushing the log
 * when pages are flushed, and therefore has a few methods that must be called by the buffer
 * manager when pages are fetched and evicted (fetchPageHook, fetchNewPageHook, and pageEvictHook).
 * These must be called from the buffer manager to ensure that pageLSN is up to date, and
 * that flushedLSN >= any pageLSN on disk.
 */
public class LogManager implements Iterable<LogRecord>, AutoCloseable {
    private BufferManager bufferManager; // 用于管理缓冲区的对象，负责页面的读取和写入。
    private Deque<Page> unflushedLogTail; // 一个双端队列，存储尚未刷新到磁盘的日志页面。
    private Page logTail; // 当前日志的尾部页面。
    private Buffer logTailBuffer; // 当前日志尾部页面的缓冲区。
    private boolean logTailPinned = false; // 标识日志尾部页面是否被固定（pinned），防止被缓冲区管理器回收。
    private long flushedLSN; // 已经刷新到磁盘的最大 LSN。

    public static final int LOG_PARTITION = 0;

    LogManager(BufferManager bufferManager) {
        this.bufferManager = bufferManager;
        this.unflushedLogTail = new ArrayDeque<>();

        this.logTail = bufferManager.fetchNewPage(new DummyLockContext("_dummyLogPageRecord"), LOG_PARTITION);
        this.unflushedLogTail.add(this.logTail);
        this.logTailBuffer = this.logTail.getBuffer();
        this.logTail.unpin();

        this.flushedLSN = maxLSN(this.logTail.getPageNum() - 1L);
    }

    /**
     * Writes to the first record in the log.
     * 功能：重写日志中的第一个记录（主记录）。
     * 实现：获取第一个页面，将新的主记录写入缓冲区，并刷新页面。
     * @param record log record to replace first record with
     */
    public synchronized void rewriteMasterRecord(MasterLogRecord record) {
        Page firstPage = bufferManager.fetchPage(new DummyLockContext("_dummyLogPageRecord"), LOG_PARTITION);
        try {
            firstPage.getBuffer().put(record.toBytes());
            firstPage.flush();
        } finally {
            firstPage.unpin();
        }
    }

    /**
     * Appends a log record to the log.
     * 功能：将新的日志记录追加到日志中。
     * 实现：如果当前日志尾部页面没有足够的空间，则分配一个新的页面。将日志记录写入缓冲区，并生成对应的 LSN。
     * @param record log record to append to the log
     * @return LSN of new log record
     */
    public synchronized long appendToLog(LogRecord record) {
        byte[] bytes = record.toBytes();
        // loop in case accessing log tail requires flushing the log in order to evict dirty page to load log tail
        //在某些情况下，为了访问日志的尾部，可能需要先刷新日志（将脏页写回磁盘），以便腾出空间加载日志尾部，这可能会导致循环操作。
        do {
            if (logTailBuffer == null || bytes.length > DiskSpaceManager.PAGE_SIZE - logTailBuffer.position()) {
                logTailPinned = true;
                logTail = bufferManager.fetchNewPage(new DummyLockContext("_dummyLogPageRecord"), LOG_PARTITION);
                unflushedLogTail.add(logTail);
                logTailBuffer = logTail.getBuffer();
            } else {
                logTailPinned = true;
                logTail.pin();
                if (logTailBuffer == null) {
                    logTail.unpin();
                }
            }
        } while (logTailBuffer == null);
        try {
            int pos = logTailBuffer.position();
            logTailBuffer.put(bytes);
            long LSN = makeLSN(unflushedLogTail.getLast().getPageNum(), pos);
            record.LSN = LSN;
            return LSN;
        } finally {
            logTail.unpin();
            logTailPinned = false;
        }
    }

    /**
     * Fetches a specific log record.
     * 功能：根据 LSN 获取特定的日志记录。
     * 实现：根据 LSN 找到对应的页面和偏移量，从缓冲区中读取日志记录。
     * @param LSN LSN of record to fetch
     * @return log record with the specified LSN
     */
    public LogRecord fetchLogRecord(long LSN) {
        try {
            Page logPage = bufferManager.fetchPage(new DummyLockContext("_dummyLogPageRecord"), getLSNPage(LSN));
            try {
                Buffer buf = logPage.getBuffer();
                buf.position(getLSNIndex(LSN));
                Optional<LogRecord> record = LogRecord.fromBytes(buf);
                record.ifPresent((LogRecord e) -> e.setLSN(LSN));
                return record.orElse(null);
            } finally {
                logPage.unpin();
            }
        } catch (PageException e) {
            return null;
        }
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     * 功能：将日志刷新到指定的 LSN。
     * 实现：遍历未刷新的日志页面，将页面刷新到磁盘，并更新 flushedLSN。
     * @param LSN LSN up to which the log should be flushed
     */
    public synchronized void flushToLSN(long LSN) {
        Iterator<Page> iter = unflushedLogTail.iterator();
        long pageNum = getLSNPage(LSN);
        while (iter.hasNext()) {
            Page page = iter.next();
            if (page.getPageNum() > pageNum) {
                break;
            }
            page.flush();
            iter.remove();
        }
        flushedLSN = Math.max(flushedLSN, maxLSN(pageNum));
        if (unflushedLogTail.size() == 0) {
            if (!logTailPinned) {
                logTail = null;
            }
            logTailBuffer = null;
        }
    }

    /**
     * @return flushedLSN
     */
    public long getFlushedLSN() {
        return flushedLSN;
    }

    /**
     * Generates LSN from log page number and index
     * @param pageNum page number of log page
     * @param index index of the log record within the log page
     * @return LSN
     */
    static long makeLSN(long pageNum, int index) {
        return DiskSpaceManager.getPageNum(pageNum) * 10000L + index;
    }

    /**
     * Generates the max possible LSN on the given page
     * @param pageNum page number of log page
     * @return max possible LSN on the log page
     */
    static long maxLSN(long pageNum) {
        return makeLSN(pageNum, 9999);
    }

    /**
     * Get the page number of the page with the record corresponding to LSN
     * @param LSN LSN to get page of
     * @return page that LSN resides on
     */
    static long getLSNPage(long LSN) {
        return LSN / 10000L;
    }

    /**
     * Get the index within the page of the record corresponding to LSN
     * @param LSN LSN to get index of
     * @return index in page that LSN resides on
     */
    static int getLSNIndex(long LSN) {
        return (int) (LSN % 10000L);
    }

    /**
     * Scan forward in the log from LSN.
     * 功能：从指定的 LSN 开始扫描日志记录。
     * 实现：返回一个迭代器，用于从指定 LSN 开始遍历日志记录。
     * @param LSN LSN to start scanning from
     * @return iterator over log entries from LSN
     */
    public Iterator<LogRecord> scanFrom(long LSN) {
        return new ConcatBacktrackingIterator<>(new LogPagesIterator(LSN));
    }

    /**
     * Scan forward in the log from the first record.
     * @return iterator over all log entries
     */
    @Override
    public Iterator<LogRecord> iterator() {
        return this.scanFrom(0);
    }

    @Override
    public synchronized void close() {
        if (!this.unflushedLogTail.isEmpty()) {
            this.flushToLSN(maxLSN(unflushedLogTail.getLast().getPageNum()));
        }
    }

    // 功能：用于迭代单个日志页面中的日志记录。
    // 实现：通过 getNextNonEmpty 和 getValue 方法实现日志记录的遍历。
    private class LogPageIterator extends IndexBacktrackingIterator<LogRecord> {
        private Page logPage;
        private int startIndex;

        private LogPageIterator(Page logPage, int startIndex) {
            super(DiskSpaceManager.PAGE_SIZE);
            this.logPage = logPage;
            this.startIndex = startIndex;
            this.logPage.unpin();
        }

        @Override
        protected int getNextNonEmpty(int currentIndex) {
            logPage.pin();
            try {
                Buffer buf = logPage.getBuffer();
                if (currentIndex == -1) {
                    currentIndex = startIndex;
                    buf.position(currentIndex);
                } else {
                    buf.position(currentIndex);
                    LogRecord.fromBytes(buf);
                    currentIndex = buf.position();
                }

                if (LogRecord.fromBytes(buf).isPresent()) {
                    return currentIndex;
                } else {
                    return DiskSpaceManager.PAGE_SIZE;
                }
            } finally {
                logPage.unpin();
            }
        }

        @Override
        protected LogRecord getValue(int index) {
            logPage.pin();
            try {
                Buffer buf = logPage.getBuffer();
                buf.position(index);
                LogRecord record = LogRecord.fromBytes(buf).orElseThrow(NoSuchElementException::new);
                record.setLSN(makeLSN(logPage.getPageNum(), index));
                return record;
            } finally {
                logPage.unpin();
            }
        }
    }

    // 功能：用于迭代多个日志页面中的日志记录。
    // 实现：通过 next 方法返回一个 LogPageIterator，用于遍历单个页面中的日志记录。
    private class LogPagesIterator implements BacktrackingIterator<BacktrackingIterable<LogRecord>> {
        private BacktrackingIterator<LogRecord> nextIter;
        private long nextIndex;

        private LogPagesIterator(long startLSN) {
            nextIndex = getLSNPage(startLSN);
            try {
                Page page = bufferManager.fetchPage(new DummyLockContext(), nextIndex);
                nextIter = new LogPageIterator(page, getLSNIndex(startLSN));
            } catch (PageException e) {
                nextIter = null;
            }
        }

        @Override
        public void markPrev() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markNext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            return nextIter != null;
        }

        @Override
        public BacktrackingIterable<LogRecord> next() {
            if (hasNext()) {
                final BacktrackingIterator<LogRecord> iter = nextIter;
                BacktrackingIterable<LogRecord> iterable = () -> iter;

                nextIter = null;
                do {
                    ++nextIndex;
                    try {
                        Page page = bufferManager.fetchPage(new DummyLockContext(), nextIndex);
                        nextIter = new LogPageIterator(page, 0);
                    } catch (PageException e) {
                        break;
                    }
                } while (!nextIter.hasNext());

                return iterable;
            }
            throw new NoSuchElementException();
        }
    }
}
