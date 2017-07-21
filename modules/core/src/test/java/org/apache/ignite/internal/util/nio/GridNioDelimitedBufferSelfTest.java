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

package org.apache.ignite.internal.util.nio;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import junit.framework.TestCase;

/**
 * Tests for {@link GridNioDelimitedBuffer}.
 */
public class GridNioDelimitedBufferSelfTest extends TestCase {
    /** */
    private static final String ASCII = "ASCII";

    /**
     * Tests simple delimiter (excluded from alphabet)
     */
    public void testReadZString() throws Exception {
        Random rnd = new Random();

        int buffSize = 0;

        byte[] delim = new byte[] {0};

        List<String> strs = new ArrayList<>(50);

        for (int i = 0; i < 50; i++) {
            int len = rnd.nextInt(128) + 1;

            buffSize += len + delim.length;

            StringBuilder sb = new StringBuilder(len);

            for (int j = 0; j < len; j++)
                sb.append((char)(rnd.nextInt(26) + 'a'));


            strs.add(sb.toString());
        }

        ByteBuffer buff = ByteBuffer.allocate(buffSize);

        for (String str : strs) {
            buff.put(str.getBytes(ASCII));
            buff.put(delim);
        }

        buff.flip();

        byte[] msg;

        GridNioDelimitedBuffer delimBuff = new GridNioDelimitedBuffer(delim);

        List<String> res = new ArrayList<>(strs.size());

        while ((msg = delimBuff.read(buff)) != null)
            res.add(new String(msg, ASCII));

        assertEquals(strs, res);
    }

    /**
     * Tests compound delimiter (included to alphabet)
     */
    public void testDelim() throws Exception {
        byte[] delim = "aabb".getBytes(ASCII);

        List<String> strs = Arrays.asList("za", "zaa", "zaab", "zab", "zaabaababbbbabaab");

        int buffSize = 0;

        for (String str : strs)
            buffSize += str.length() + delim.length;

        ByteBuffer buff = ByteBuffer.allocate(buffSize);

        for (String str : strs) {
            buff.put(str.getBytes(ASCII));
            buff.put(delim);
        }

        buff.flip();

        byte[] msg;

        GridNioDelimitedBuffer delimBuff = new GridNioDelimitedBuffer(delim);

        List<String> res = new ArrayList<>(strs.size());

        while ((msg = delimBuff.read(buff)) != null)
            res.add(new String(msg, ASCII));

        assertEquals(strs, res);
    }
}