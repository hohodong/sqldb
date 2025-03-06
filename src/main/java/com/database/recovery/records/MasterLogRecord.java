package com.database.recovery.records;

import com.database.common.Buffer;
import com.database.common.ByteBuffer;
import com.database.recovery.LogRecord;
import com.database.recovery.LogType;

import java.util.Objects;
import java.util.Optional;

public class MasterLogRecord extends LogRecord {
    public long lastCheckpointLSN;

    public MasterLogRecord(long lastCheckpointLSN) {
        super(LogType.MASTER);
        this.lastCheckpointLSN = lastCheckpointLSN;
    }

    @Override
    public byte[] toBytes() {
        byte[] b = new byte[1 + Long.BYTES];
        ByteBuffer.wrap(b).put((byte) getType().getValue()).putLong(lastCheckpointLSN);
        return b;
    }

    public static Optional<LogRecord> fromBytes(Buffer buf) {
        return Optional.of(new MasterLogRecord(buf.getLong()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        MasterLogRecord that = (MasterLogRecord) o;
        return lastCheckpointLSN == that.lastCheckpointLSN;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), lastCheckpointLSN);
    }

    @Override
    public String toString() {
        return "MasterLogRecord{" +
               "lastCheckpointLSN=" + lastCheckpointLSN +
               ", LSN=" + LSN +
               '}';
    }
}
