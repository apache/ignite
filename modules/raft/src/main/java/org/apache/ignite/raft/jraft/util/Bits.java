/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

/**
 * Bits util.
 *
 * @author jiachun.fjc
 */
public class Bits {

    public static int getInt(final byte[] b, final int off) {
        return HeapByteBufUtil.getInt(b, off);
    }

    public static long getLong(final byte[] b, final int off) {
        return HeapByteBufUtil.getLong(b, off);
    }

    public static void putInt(final byte[] b, final int off, final int val) {
        HeapByteBufUtil.setInt(b, off, val);
    }

    public static void putShort(final byte[] b, final int off, final short val) {
        HeapByteBufUtil.setShort(b, off, val);
    }

    public static short getShort(final byte[] b, final int off) {
        return HeapByteBufUtil.getShort(b, off);
    }

    public static void putLong(final byte[] b, final int off, final long val) {
        HeapByteBufUtil.setLong(b, off, val);
    }
}
