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

public class UndoFreePageLogRecord extends LogRecord {
    private long transNum;
    private long pageNum;
    private long prevLSN;
    private long undoNextLSN;

    public UndoFreePageLogRecord(long transNum, long pageNum, long prevLSN, long undoNextLSN) {
        super(LogType.UNDO_FREE_PAGE);
        this.transNum = transNum;
        this.pageNum = pageNum;
        this.prevLSN = prevLSN;
        this.undoNextLSN = undoNextLSN;
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
    public Optional<Long> getUndoNextLSN() {
        return Optional.of(undoNextLSN);
    }

    @Override
    public boolean isRedoable() {
        return true;
    }

    @Override
    public void redo(RecoveryManager rm, DiskSpaceManager dsm, BufferManager bm) {
        // Allocated page will appear on disk after allocPage is called,
        // so we must flush up to this record before calling it.
        rm.flushToLSN(getLSN());
        super.redo(rm, dsm, bm);
        try {
            dsm.allocPage(pageNum);
        } catch (IllegalStateException e) {
            /* do nothing - page already exists */
        }
    }

    @Override
    public byte[] toBytes() {
        byte[] b = new byte[1 + Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES];
        ByteBuffer.wrap(b)
        .put((byte) getType().getValue())
        .putLong(transNum)
        .putLong(pageNum)
        .putLong(prevLSN)
        .putLong(undoNextLSN);
        return b;
    }

    public static Optional<LogRecord> fromBytes(Buffer buf) {
        long transNum = buf.getLong();
        long pageNum = buf.getLong();
        long prevLSN = buf.getLong();
        long undoNextLSN = buf.getLong();
        return Optional.of(new UndoFreePageLogRecord(transNum, pageNum, prevLSN, undoNextLSN));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        UndoFreePageLogRecord that = (UndoFreePageLogRecord) o;
        return transNum == that.transNum &&
               pageNum == that.pageNum &&
               prevLSN == that.prevLSN &&
               undoNextLSN == that.undoNextLSN;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), transNum, pageNum, prevLSN, undoNextLSN);
    }

    @Override
    public String toString() {
        return "UndoFreePageLogRecord{" +
               "transNum=" + transNum +
               ", pageNum=" + pageNum +
               ", prevLSN=" + prevLSN +
               ", undoNextLSN=" + undoNextLSN +
               ", LSN=" + LSN +
               '}';
    }
}
