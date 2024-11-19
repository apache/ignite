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

package org.apache.ignite.internal.jdbc2;

import java.io.IOException;
import org.junit.Test;

import static org.apache.ignite.internal.binary.streams.BinaryAbstractOutputStream.MAX_ARRAY_SIZE;
import static org.apache.ignite.internal.jdbc2.JdbcBinaryBuffer.MIN_CAP;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

/** */
public class JdbcBinaryBufferTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCapacity() throws Exception {
        assertEquals(MIN_CAP, JdbcBinaryBuffer.capacity(10, 20));

        assertEquals(MAX_ARRAY_SIZE, JdbcBinaryBuffer.capacity(10, MAX_ARRAY_SIZE));

        assertEquals(MIN_CAP * 16, JdbcBinaryBuffer.capacity(MIN_CAP, MIN_CAP * 10));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWriteTooMuchData() throws Exception {
        JdbcBinaryBuffer buf = JdbcBinaryBuffer.createReadWrite(new byte[10]);

        assertThrows(null, () -> {
            buf.write(MAX_ARRAY_SIZE, 1);

            return null;
        }, IOException.class, "Too much data. Can't write more then 2147483639 bytes.");
    }
}
