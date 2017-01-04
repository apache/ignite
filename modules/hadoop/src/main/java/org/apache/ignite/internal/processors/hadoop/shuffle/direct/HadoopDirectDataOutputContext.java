/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.shuffle.direct;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * Hadoop data output context for direct communication.
 */
public class HadoopDirectDataOutputContext {
    /** Flush size. */
    private final int flushSize;

    /** Whether to perform GZIP. */
    private final boolean gzip;

    /** Key serialization. */
    private final HadoopSerialization keySer;

    /** Value serialization. */
    private final HadoopSerialization valSer;

    /** Data output. */
    private HadoopDirectDataOutput out;

    /** Number of keys written. */
    private int cnt;

    /**
     * Constructor.
     *
     * @param flushSize Flush size.
     * @param gzip Whether to perform GZIP.
     * @param taskCtx Task context.
     * @throws IgniteCheckedException If failed.
     */
    public HadoopDirectDataOutputContext(int flushSize, boolean gzip, HadoopTaskContext taskCtx)
        throws IgniteCheckedException {
        this.flushSize = flushSize;
        this.gzip = gzip;

        keySer = taskCtx.keySerialization();
        valSer = taskCtx.valueSerialization();

        out = new HadoopDirectDataOutput(flushSize);
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
    public HadoopDirectDataOutputState state() {
        if (gzip) {
            try {
                HadoopDirectDataOutput gzipOut = new HadoopDirectDataOutput(out.position());

                // TODO: Buf size to config.
                try (GZIPOutputStream gzip = new GZIPOutputStream(gzipOut, 8192)) {
                    gzip.write(out.buffer(), 0, out.position());
                }

                return new HadoopDirectDataOutputState(gzipOut.buffer(), gzipOut.position(), out.position());
            }
            catch (IOException e) {
                throw new IgniteException("Failed to compress.", e);
            }
        }
        else
            return new HadoopDirectDataOutputState(out.buffer(), out.position(), out.position());
    }

    /**
     * Reset buffer.
     */
    public void reset() {
        int allocSize = Math.max(flushSize, out.position());

        out = new HadoopDirectDataOutput(flushSize, allocSize);
        cnt = 0;
    }
}
