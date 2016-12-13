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

package org.apache.ignite.internal.processors.hadoop.shuffle;

/**
 * Raw value wrapper.
 */
public class HeapWrapper {
    /** Array. */
    private final byte[] arr;

    /** Position. */
    private int pos;

    /** Length. */
    private int len;

    /**
     * Constructor.
     *
     * @param arr Array.
     */
    public HeapWrapper(byte[] arr) {
        this.arr = arr;
    }

    /**
     * @return Array.
     */
    public byte[] array() {
        return arr;
    }

    /**
     * @return Position.
     */
    public int position() {
        return pos;
    }

    /**
     * @return Length.
     */
    public int length() {
        return len;
    }

    /**
     * Set position and length.
     *
     * @param pos Position.
     * @param len Length.
     */
    public void set(int pos, int len) {
        this.pos = pos;
        this.len = len;
    }
}
