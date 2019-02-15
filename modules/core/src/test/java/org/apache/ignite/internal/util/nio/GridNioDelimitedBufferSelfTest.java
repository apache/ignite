/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util.nio;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link GridNioDelimitedBuffer}.
 */
public class GridNioDelimitedBufferSelfTest {
    /** */
    private static final String ASCII = "ASCII";

    /**
     * Tests simple delimiter (excluded from alphabet)
     */
    @Test
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
    @Test
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
