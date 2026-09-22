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

package org.apache.ignite.internal.util;

import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Cache object utility methods.
 */
public class CacheObjectUnsafeUtils {
    /**
     * @param addr Write address.
     * @param type Object type.
     * @param valBytes Value bytes array.
     * @return Offset shift compared to initial address.
     */
    public static int putValue(long addr, byte type, byte[] valBytes) {
        return putValue(addr, type, valBytes, 0, valBytes.length);
    }

    /**
     * @param addr Write address.
     * @param type Object type.
     * @param srcBytes Source value bytes array.
     * @param srcOff Start position in sourceBytes.
     * @param len Number of bytes for write.
     * @return Offset shift compared to initial address.
     */
    public static int putValue(long addr, byte type, byte[] srcBytes, int srcOff, int len) {
        int off = 0;

        PageUtils.putInt(addr, off, len);
        off += 4;

        PageUtils.putByte(addr, off, type);
        off++;

        PageUtils.putBytes(addr, off, srcBytes, srcOff, len);
        off += len;

        return off;
    }
}
