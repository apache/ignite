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

/**
 * Hadoop data output state for direct communication.
 */
public class HadoopDirectDataOutputState {
    /** Buffer. */
    private final byte[] buf;

    /** Buffer length. */
    private final int bufLen;

    /** Data length. */
    private final int dataLen;

    /**
     * Constructor.
     *
     * @param buf Buffer.
     * @param bufLen Buffer length.
     * @param dataLen Data length.
     */
    public HadoopDirectDataOutputState(byte[] buf, int bufLen, int dataLen) {
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
