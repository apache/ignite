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

package org.apache.ignite.internal.processors.hadoop.igfs;

import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS client future that holds response parse closure.
 */
public class HadoopIgfsFuture<T> extends GridFutureAdapter<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Output buffer. */
    private byte[] outBuf;

    /** Output offset. */
    private int outOff;

    /** Output length. */
    private int outLen;

    /** Read future flag. */
    private boolean read;

    /**
     * @return Output buffer.
     */
    public byte[] outputBuffer() {
        return outBuf;
    }

    /**
     * @param outBuf Output buffer.
     */
    public void outputBuffer(@Nullable byte[] outBuf) {
        this.outBuf = outBuf;
    }

    /**
     * @return Offset in output buffer to write from.
     */
    public int outputOffset() {
        return outOff;
    }

    /**
     * @param outOff Offset in output buffer to write from.
     */
    public void outputOffset(int outOff) {
        this.outOff = outOff;
    }

    /**
     * @return Length to write to output buffer.
     */
    public int outputLength() {
        return outLen;
    }

    /**
     * @param outLen Length to write to output buffer.
     */
    public void outputLength(int outLen) {
        this.outLen = outLen;
    }

    /**
     * @param read {@code True} if this is a read future.
     */
    public void read(boolean read) {
        this.read = read;
    }

    /**
     * @return {@code True} if this is a read future.
     */
    public boolean read() {
        return read;
    }
}