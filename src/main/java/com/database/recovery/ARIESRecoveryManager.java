package com.database.recovery;

import com.database.Transaction;
import com.database.recovery.records.*;
import com.database.common.Pair;
import com.database.concurrency.DummyLockContext;
import com.database.io.DiskSpaceManager;
import com.database.memory.BufferManager;
import com.database.memory.Page;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord commitLogRecord = new CommitTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(commitLogRecord);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Update status
        transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord abortLogRecord = new AbortTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(abortLogRecord);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Update status
        transactionEntry.transaction.setStatus(Transaction.Status.ABORTING);
        return LSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        if(transactionEntry.transaction.getStatus() == Transaction.Status.ABORTING){
            this.rollbackToLSN(transNum, 0);
        }
        long prevLSN = transactionEntry.lastLSN;
        LogRecord endLogRecord = new EndTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(endLogRecord);
        // Update the transaction table
        transactionTable.remove(transNum);
        // Update status
        transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        return LSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // TODO(proj5) implement the rollback logic described above
        while(currentLSN > LSN){
            LogRecord currentLogRecord = logManager.fetchLogRecord(currentLSN);
            if(currentLogRecord.isUndoable()){
                LogRecord clr = currentLogRecord.undo(transactionEntry.lastLSN);
                logManager.appendToLog(clr);
                transactionEntry.lastLSN = clr.getLSN();
                clr.redo(this, this.diskSpaceManager, this.bufferManager);
            }
            currentLSN = currentLogRecord.getPrevLSN().orElse(LSN);
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Update DPT
        if(!dirtyPageTable.containsKey(pageNum)){
            dirtyPageTable.put(pageNum, LSN);
        }
        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        this.rollbackToLSN(transNum, savepointLSN);
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     * 这是一个同步方法（synchronized），确保在多线程环境下只有一个线程可以执行检查点操作。
     * 该方法的主要任务是创建一个检查点，并将相关的日志记录写入日志文件中。
     * 检查点记录了数据库在某一时刻的状态，包括事务的状态和脏页表（Dirty Page Table, DPT）等信息。
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        // 首先，代码创建了一个BeginCheckpointLogRecord对象，表示检查点的开始。
        // 这个记录会被追加到日志中，并返回其日志序列号（LSN, Log Sequence Number），即beginLSN。
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        // chkptDPT 是一个HashMap，用于存储脏页表（Dirty Page Table, DPT）的信息。DPT记录了哪些页是脏页（即被修改过但还未写回磁盘的页）。
        // chkptTxnTable 也是一个HashMap，用于存储事务表（Transaction Table）的信息。事务表记录了每个事务的状态和最后一次操作的LSN。
        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        for(Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()){
            Long pageID = entry.getKey();
            Long recLSN = entry.getValue();
            if(EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size() + 1, 0)){
                chkptDPT.put(pageID, recLSN);
            } else {
                LogRecord endCheckpointLogRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endCheckpointLogRecord);
                chkptDPT.clear();
                chkptDPT.put(pageID, recLSN);
            }
        }

        for(Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()){
            Long transNum = entry.getKey();
            Transaction.Status status = entry.getValue().transaction.getStatus();
            Long lastLSN = entry.getValue().lastLSN;
            if(EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size() + 1)){
                chkptTxnTable.put(transNum, new Pair<>(status, lastLSN));
            } else {
                LogRecord endCheckpointLogRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endCheckpointLogRecord);
                chkptDPT.clear();
                chkptTxnTable.clear();
                chkptTxnTable.put(transNum, new Pair<>(status, lastLSN));
            }
        }
//        if(!chkptDPT.isEmpty() || !chkptTxnTable.isEmpty()){
//            LogRecord endCheckpointLogRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
//            logManager.appendToLog(endCheckpointLogRecord);
//            chkptDPT.clear();
//            chkptTxnTable.clear();
//        }


        // Last end checkpoint record
        // 创建一个EndCheckpointLogRecord对象，表示检查点的结束。
        // 这个记录包含了chkptDPT和chkptTxnTable的信息，并将其追加到日志中。
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        // 在写入结束检查点记录后，代码调用flushToLSN()方法，确保日志记录被刷新到磁盘。这是为了保证在系统崩溃时，检查点的信息不会丢失。
        flushToLSN(endRecord.getLSN());

        // Update master record
        // 最后，创建一个MasterLogRecord对象，记录了检查点的开始LSN（即beginLSN）。
        // 这个主记录会被重写到日志中，以便在数据库恢复时能够找到最近的检查点。
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement
        Iterator<LogRecord> logRecordIterator = logManager.scanFrom(LSN);
        while(logRecordIterator.hasNext()){
            LogRecord curLogRecord = logRecordIterator.next();
            LogType logType = curLogRecord.getType();
            if(curLogRecord.getTransNum().isPresent()){
                Long transNum = curLogRecord.getTransNum().get();
                if(!transactionTable.containsKey(transNum)){
                    startTransaction(newTransaction.apply(transNum));
                }
                TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
                transactionTableEntry.lastLSN = curLogRecord.getLSN();
            }
            if(curLogRecord.getPageNum().isPresent()){
                Long pageNum = curLogRecord.getPageNum().get();
                if(logType.equals(LogType.UPDATE_PAGE) || logType.equals(LogType.UNDO_UPDATE_PAGE)){
                    dirtyPage(pageNum, curLogRecord.getLSN());
                } else if(logType.equals(LogType.FREE_PAGE) || logType.equals(LogType.UNDO_ALLOC_PAGE)){
                    dirtyPageTable.remove(pageNum);
                }
            }
            if(logType.equals(LogType.COMMIT_TRANSACTION) || logType.equals(LogType.ABORT_TRANSACTION) || logType.equals(LogType.END_TRANSACTION)){
                Long transNum = curLogRecord.getTransNum().get();
                if(!transactionTable.containsKey(transNum)){
                    startTransaction(newTransaction.apply(transNum));
                }
                if(logType.equals(LogType.COMMIT_TRANSACTION)){
                    transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
                } else if(logType.equals(LogType.ABORT_TRANSACTION)){
                    transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                } else {
                    TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
                    transactionTableEntry.transaction.cleanup();
                    transactionTableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                    transactionTable.remove(transNum);
                    endedTransactions.add(transNum);
                }
            }
            if(logType.equals(LogType.END_CHECKPOINT)){
                Map<Long, Long> ckpLogRecordDirtyPageTable = curLogRecord.getDirtyPageTable();
                Map<Long, Pair<Transaction.Status, Long>> ckpLogRecordTransactionTable = curLogRecord.getTransactionTable();
                dirtyPageTable.putAll(ckpLogRecordDirtyPageTable);
                for(Map.Entry<Long, Pair<Transaction.Status, Long>> entry : ckpLogRecordTransactionTable.entrySet()){
                    Long txn = entry.getKey();
                    if(endedTransactions.contains(txn)){
                        continue;
                    }
                    if(!transactionTable.containsKey(txn)) {
                        startTransaction(newTransaction.apply(txn));
                    }
                    transactionTable.get(txn).lastLSN = Math.max(transactionTable.get(txn).lastLSN, entry.getValue().getSecond());
                    Transaction.Status curStatus = transactionTable.get(txn).transaction.getStatus();
                    Transaction.Status ckpStatus = entry.getValue().getFirst();
                    if(curStatus.equals(Transaction.Status.RUNNING)){
                        if(ckpStatus.equals(Transaction.Status.COMMITTING)){
                            transactionTable.get(txn).transaction.setStatus(ckpStatus);
                        } else if (ckpStatus.equals(Transaction.Status.ABORTING)) {
                            transactionTable.get(txn).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        }
                    } else if (curStatus.equals(Transaction.Status.ABORTING)) {
                        if(ckpStatus.equals(Transaction.Status.COMPLETE)){
                            transactionTable.get(txn).transaction.setStatus(ckpStatus);
                        }
                    } else if (curStatus.equals(Transaction.Status.COMMITTING)) {
                        if(ckpStatus.equals(Transaction.Status.COMPLETE)){
                            transactionTable.get(txn).transaction.setStatus(ckpStatus);
                        }
                    }
                }
            }
        }
        for(Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()){
            Long txn = entry.getKey();
            Transaction.Status curStatus = entry.getValue().transaction.getStatus();
            if(curStatus.equals(Transaction.Status.RUNNING)){
                entry.getValue().transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                LogRecord endRecord = new AbortTransactionLogRecord(txn, entry.getValue().lastLSN);
                logManager.appendToLog(endRecord);
                entry.getValue().lastLSN = endRecord.getLSN();
            } else if (curStatus.equals(Transaction.Status.COMMITTING)) {
                entry.getValue().transaction.cleanup();
                entry.getValue().transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(txn);
                LogRecord endRecord = new EndTransactionLogRecord(txn, entry.getValue().lastLSN);
                logManager.appendToLog(endRecord);
                entry.getValue().lastLSN = endRecord.getLSN();
            }
        }
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // TODO(proj5): implement
        Long startLSN = Long.MAX_VALUE;
        if(dirtyPageTable.isEmpty()){
            return;
        }
        for(Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()){
            startLSN = Math.min(startLSN, entry.getValue());
        }
        Iterator<LogRecord> logRecordIterator = logManager.scanFrom(startLSN);
        while(logRecordIterator.hasNext()) {
            LogRecord curLogRecord = logRecordIterator.next();
            LogType logType = curLogRecord.getType();
            if(!curLogRecord.isRedoable()){
                continue;
            }
            if(logType.equals(LogType.ALLOC_PART) || logType.equals(LogType.FREE_PART) || logType.equals(LogType.UNDO_ALLOC_PART) || logType.equals(LogType.UNDO_FREE_PART)){
                curLogRecord.redo(this, diskSpaceManager, bufferManager);
            } else if (logType.equals(LogType.ALLOC_PAGE) || logType.equals(LogType.UNDO_FREE_PAGE)) {
                curLogRecord.redo(this, diskSpaceManager, bufferManager);
            } else if (logType.equals(LogType.UPDATE_PAGE) || logType.equals(LogType.UNDO_UPDATE_PAGE) || logType.equals(LogType.FREE_PAGE) || logType.equals(LogType.UNDO_ALLOC_PAGE)) {
                Long pageNum = curLogRecord.getPageNum().get(); // pageNum 一定存在，省略 isPresent 检查
                Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum);
                long pageLSN;
                try {
                    pageLSN = page.getPageLSN();
                } finally {
                    page.unpin();
                }
                if(!dirtyPageTable.containsKey(pageNum)){
                    continue;
                } else if(dirtyPageTable.get(pageNum) > curLogRecord.getLSN()){
                    continue;
                } else if (pageLSN >= curLogRecord.getLSN()) {
                    continue;
                } else {
                    curLogRecord.redo(this, diskSpaceManager, bufferManager);
                }
            }
        }
        return;
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        PriorityQueue<Pair<Long, TransactionTableEntry>> pq = new PriorityQueue<>(new PairFirstReverseComparator<>());
        for(Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()){
            if(entry.getValue().transaction.getStatus().equals(Transaction.Status.RECOVERY_ABORTING)){
                pq.add(new Pair<>(entry.getValue().lastLSN, entry.getValue()));
            }
        }
        while(!pq.isEmpty()){
            Long curLSN = pq.peek().getFirst();
            TransactionTableEntry transactionTableEntry = pq.peek().getSecond();
            pq.poll();
            LogRecord curLogRecord = logManager.fetchLogRecord(curLSN);
            if(curLogRecord.isUndoable()){
                LogRecord clr = curLogRecord.undo(transactionTableEntry.lastLSN);
                transactionTableEntry.lastLSN = logManager.appendToLog(clr);
                clr.redo(this, diskSpaceManager, bufferManager);
            }
            long preLSN = -1L;
            if(curLogRecord.getUndoNextLSN().isPresent()){
                preLSN = curLogRecord.getUndoNextLSN().get();
                pq.add(new Pair<>(preLSN, transactionTableEntry));
            } else if (curLogRecord.getPrevLSN().isPresent()) {
                preLSN = curLogRecord.getPrevLSN().get();
                pq.add(new Pair<>(preLSN, transactionTableEntry));
            }
            if(preLSN == 0){
                transactionTableEntry.transaction.cleanup();
                transactionTableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(transactionTableEntry.transaction.getTransNum());
                LogRecord endLogRecord = new EndTransactionLogRecord(transactionTableEntry.transaction.getTransNum(), transactionTableEntry.lastLSN);
                logManager.appendToLog(endLogRecord);
            }
        }
        return;
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
