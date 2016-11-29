package org.apache.ignite.internal.processors.hadoop.shuffle.streams;

/**
 * Hadoop
 */
public class NewHadoopDataOutputState {
    /** Buffer. */
    private final byte[] buf;

    /** Buffer length. */
    private final int bufLen;

    /** Original data length. */
    private final int dataLen;

    /**
     * Constructor.
     *
     * @param buf Buffer.
     * @param bufLen Buffer length.
     * @param dataLen Original length.
     */
    public NewHadoopDataOutputState(byte[] buf, int bufLen, int dataLen) {
        this.buf = buf;
        this.bufLen = bufLen;
        this.dataLen = dataLen;
    }

    /**
     * @return Buffer.
     */
    public byte[] buffer() {
        return buf;
    }

    /**
     * @return Length.
     */
    public int bufferLength() {
        return bufLen;
    }

    /**
     * @return Original data length.
     */
    public int dataLength() {
        return dataLen;
    }
}
