package org.apache.ignite.internal.pagemem.wal.record;

public class ExchangeRecord extends TimeStampRecord {
    /** Event. */
    private Short constId;

    /** Type. */
    private Type type;

    /**
     * @param constId Const id.
     * @param type Type.
     * @param timeStamp TimeStamp.
     */
    public ExchangeRecord(Short constId, Type type, long timeStamp) {
        super(timeStamp);

        this.constId = constId;
        this.type = type;
    }

    /**
     * @param constId Const id.
     * @param type Type.
     */
    public ExchangeRecord(Short constId, Type type) {
        this.constId = constId;
        this.type = type;
    }


    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.EXCHANGE;
    }

    /**
     *
     */
    public Short getConstId() {
        return constId;
    }

    /**
     *
     */
    public Type getType() {
        return type;
    }

    public enum Type {
        /** Join. */
        JOIN,
        /** Left. */
        LEFT
    }

    @Override public String toString() {
        return "ExchangeRecord[" +
            "constId='" + constId + '\'' +
            ", type=" + type +
            ", timestamp=" + timestamp +
            ']';
    }
}
