package com.database.recovery.records;

import com.database.common.Buffer;
import com.database.common.ByteBuffer;
import com.database.io.DiskSpaceManager;
import com.database.memory.BufferManager;
import com.database.recovery.LogRecord;
import com.database.recovery.LogType;
import com.database.recovery.RecoveryManager;

import java.util.Objects;
import java.util.Optional;

/**
 * A log entry that records the allocation of a page
 */
public class AllocPageLogRecord extends LogRecord {
    private long transNum;
    private long pageNum;
    private long prevLSN;

    public AllocPageLogRecord(long transNum, long pageNum, long prevLSN) {
        super(LogType.ALLOC_PAGE);
        this.transNum = transNum;
        this.pageNum = pageNum;
        this.prevLSN = prevLSN;
    }

    @Override
    public Optional<Long> getTransNum() {
        return Optional.of(transNum);
    }

    @Override
    public Optional<Long> getPrevLSN() {
        return Optional.of(prevLSN);
    }

    @Override
    public Optional<Long> getPageNum() {
        return Optional.of(pageNum);
    }

    @Override
    public boolean isUndoable() {
        return true;
    }

    @Override
    public boolean isRedoable() {
        return true;
    }

    @Override
    public LogRecord undo(long lastLSN) {
        return new UndoAllocPageLogRecord(transNum, pageNum, lastLSN, prevLSN);
    }

    @Override
    public void redo(RecoveryManager rm, DiskSpaceManager dsm, BufferManager bm) {
        super.redo(rm, dsm, bm);

        try {
            dsm.allocPage(pageNum);
        } catch (IllegalStateException e) {
            /* do nothing - page already exists */
        }
    }

    @Override
    public byte[] toBytes() {
        byte[] b = new byte[1 + Long.BYTES + Long.BYTES + Long.BYTES];
        ByteBuffer.wrap(b)
        .put((byte) getType().getValue())
        .putLong(transNum)
        .putLong(pageNum)
        .putLong(prevLSN);
        return b;
    }

    public static Optional<LogRecord> fromBytes(Buffer buf) {
        long transNum = buf.getLong();
        long pageNum = buf.getLong();
        long prevLSN = buf.getLong();
        return Optional.of(new AllocPageLogRecord(transNum, pageNum, prevLSN));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        AllocPageLogRecord that = (AllocPageLogRecord) o;
        return transNum == that.transNum &&
               pageNum == that.pageNum &&
               prevLSN == that.prevLSN;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), transNum, pageNum, prevLSN);
    }

    @Override
    public String toString() {
        return "AllocPageLogRecord{" +
               "transNum=" + transNum +
               ", pageNum=" + pageNum +
               ", prevLSN=" + prevLSN +
               ", LSN=" + LSN +
               '}';
    }
}
