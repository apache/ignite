package org.apache.ignite.internal.processors.hadoop.shuffle.streams;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * Hadoop data output context.
 */
public class NewHadoopDataOutputContext {
    /** Flush size. */
    private final int flushSize;

    /** GZIP data output. */
    private final boolean gzip;

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
     * @param gzip Whether to use GZIP.
     * @param taskCtx Task context.
     * @throws IgniteCheckedException If failed.
     */
    public NewHadoopDataOutputContext(int flushSize, boolean gzip, HadoopTaskContext taskCtx)
        throws IgniteCheckedException {
        this.flushSize = flushSize;
        this.gzip = gzip;

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
    public boolean write(Object key, Object val) throws IgniteCheckedException {
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
     * @return State.
     */
    public NewHadoopDataOutputState state() {
        if (gzip) {
            try {
                NewHadoopDataOutput gzipOut = new NewHadoopDataOutput(out.position());

                // TODO: Buf size to config.
                try (GZIPOutputStream gzip = new GZIPOutputStream(gzipOut, 8192)) {
                    gzip.write(out.buffer(), 0, out.position());
                }

                //System.out.println("COMPRESSED [" + out.position() + ", " + gzipOut.position() + ']');

                return new NewHadoopDataOutputState(gzipOut.buffer(), gzipOut.position(), out.position());
            }
            catch (IOException e) {
                throw new IgniteException("Failed to compress.", e);
            }
        }
        else
            return new NewHadoopDataOutputState(out.buffer(), out.position(), out.position());
    }

    /**
     * Reset buffer.
     */
    public void reset() {
        int allocSize = Math.max(flushSize, out.position());

        out = new NewHadoopDataOutput(flushSize, allocSize);
        cnt = 0;
    }
}
