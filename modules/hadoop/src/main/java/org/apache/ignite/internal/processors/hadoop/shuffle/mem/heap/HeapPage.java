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

package org.apache.ignite.internal.processors.hadoop.shuffle.mem.heap;

/**
 * Page.
 */
public class HeapPage {
    /** Pointer. */
    private byte [] buf;

    /** Size. */
    private final int order;

    /** Size. */
    private final long size;

    /**
     * Constructor.
     *
     * @param order Page order.
     * @param size Size.
     */
    HeapPage(int order, int size) {
        this.order = order;
        this.size = size;

        buf = new byte[size];
    }

    /**
     * @return Pointer.
     */
    long ptr() {
        return ((long)order) << 32;
    }

    /**
     * @return Page size.
     */
    long size() {
        return size;
    }

    /**
     * @return Page buffer.
     */
    byte[] buf() {
        return buf;
    }
}
