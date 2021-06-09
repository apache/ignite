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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BitsTest {

    @Test
    public void testGetSet() {
        byte[] bs = new byte[1 + 2 + 4 + 8];

        bs[0] = (byte) 1;
        Bits.putShort(bs, 1, (short) 2);
        Bits.putInt(bs, 3, 3);
        Bits.putLong(bs, 7, 99L);

        assertEquals(1, bs[0]);
        assertEquals((short) 2, Bits.getShort(bs, 1));
        assertEquals(3, Bits.getInt(bs, 3));
        assertEquals(99L, Bits.getLong(bs, 7));
    }
}
