package org.apache.ignite.internal.processors.hadoop.shuffle.streams;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;

/**
 * Hadoop data output context.
 */
public class NewHadoopDataOutputContext {
    /** Flush size. */
    private final int flushSize;

    /** Key serialization. */
    private final HadoopSerialization keySer;

    /** Value serialization. */
    private final HadoopSerialization valSer;

    /** Data output. */
    private NewHadoopDataOutput out;

    /** Number of keys written. */
    private int cnt;

    /**
     * Constructor.
     *
     * @param flushSize Flush size.
     * @param taskCtx Task context.
     * @throws IgniteCheckedException If failed.
     */
    public NewHadoopDataOutputContext(int flushSize, HadoopTaskContext taskCtx) throws IgniteCheckedException {
        this.flushSize = flushSize;

        keySer = taskCtx.keySerialization();
        valSer = taskCtx.valueSerialization();

        out = new NewHadoopDataOutput(flushSize);
    }

    /**
     * Write key-value pair.
     *
     * @param key Key.
     * @param val Value.
     * @return Whether flush is needed.
     * @throws IgniteCheckedException If failed.
     */
    private boolean write(Object key, Object val) throws IgniteCheckedException {
        keySer.write(out, key);
        valSer.write(out, val);

        cnt++;

        return out.readyForFlush();
    }

    /**
     * @return Key-value pairs count.
     */
    public int count() {
        return cnt;
    }

    /**
     * @return Buffer.
     */
    public byte[] buffer() {
        return out.buffer();
    }

    /**
     * Reset buffer.
     */
    public void reset() {
        int allocSize = Math.max(flushSize, out.position());

        out = new NewHadoopDataOutput(flushSize, allocSize);
    }
}
