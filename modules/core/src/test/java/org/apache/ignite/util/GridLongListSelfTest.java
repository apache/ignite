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

package org.apache.ignite.util;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class GridLongListSelfTest {
    /**
     *
     */
    @Test
    public void testTruncate() {
        GridLongList list = asList(1, 2, 3, 4, 5, 6, 7, 8);

        list.truncate(4, true);

        assertEquals(asList(1, 2, 3, 4), list);

        list.truncate(2, false);

        assertEquals(asList(3, 4), list);

        list = new GridLongList();

        list.truncate(0, false);
        list.truncate(0, true);

        assertEquals(new GridLongList(), list);
    }

    /**
     *
     */
    @Test
    public void testRemove() {
        GridLongList list = asList(1, 2, 3, 4, 5, 6);

        assertEquals(2, list.removeValue(0, 3));
        assertEquals(asList(1, 2, 4, 5, 6), list);

        assertEquals(-1, list.removeValue(1, 1));
        assertEquals(-1, list.removeValue(0, 3));

        assertEquals(4, list.removeValue(0, 6));
        assertEquals(asList(1, 2, 4, 5), list);

        assertEquals(2, list.removeIndex(1));
        assertEquals(asList(1, 4, 5), list);

        assertEquals(1, list.removeIndex(0));
        assertEquals(asList(4, 5), list);
    }

    /**
     *
     */
    @Test
    public void testSort() {
        assertEquals(new GridLongList(), new GridLongList().sort());
        assertEquals(asList(1), asList(1).sort());
        assertEquals(asList(1, 2), asList(2, 1).sort());
        assertEquals(asList(1, 2, 3), asList(2, 1, 3).sort());

        GridLongList list = new GridLongList();

        list.add(4);
        list.add(3);
        list.add(5);
        list.add(1);

        assertEquals(asList(1, 3, 4, 5), list.sort());

        list.add(0);

        assertEquals(asList(1, 3, 4, 5, 0), list);
        assertEquals(asList(0, 1, 3, 4, 5), list.sort());
    }

    /**
     *
     */
    @Test
    public void testArrayCopy() {
        GridLongList list = new GridLongList();

        long[] arr = list.arrayCopy();

        assertNotNull(arr);

        assertEquals(0, arr.length);

        list.add(1L);

        arr = list.arrayCopy();

        assertNotNull(arr);

        assertEquals(1, arr.length);

        assertEquals(1L, arr[0]);
    }

    /** */
    @Test
    public void testSerializationDefaultConstructor() {
        MessageWriter writer = new DirectMessageWriter(null);

        ByteBuffer buf = ByteBuffer.allocate(4096);
        
        writer.setBuffer(buf);

        GridLongList ll = new GridLongList();

        {
            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(1 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.add(2L);
            ll.add(4L);

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(17 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(9 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(1 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            for (int i = 0; i < 300; i++)
                ll.add(i);

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(300, ll.size());

            Assert.assertEquals(2402 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.clear();

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(1 /* array */, buf.position());
        }
    }

    /** */
    @Test
    public void testSerializationConstructorWithSize() {
        MessageWriter writer = new DirectMessageWriter(null);

        ByteBuffer buf = ByteBuffer.allocate(4096);

        writer.setBuffer(buf);

        GridLongList ll = new GridLongList(5);

        {
            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(1 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.add(2L);
            ll.add(4L);

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(17 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(9 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(1 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            for (int i = 0; i < 300; i++)
                ll.add(i);

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(300, ll.size());

            Assert.assertEquals(2402 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.clear();

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(1 /* array */, buf.position());
        }
    }

    /** */
    @Test
    public void testSerializationConstructorWithZeroSize() {
        MessageWriter writer = new DirectMessageWriter(null);

        ByteBuffer buf = ByteBuffer.allocate(4096);

        writer.setBuffer(buf);

        GridLongList ll = new GridLongList(0);

        {
            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(1 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.add(2L);
            ll.add(4L);

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(17 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(9 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(1 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            for (int i = 0; i < 300; i++)
                ll.add(i);

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(300, ll.size());

            Assert.assertEquals(2402 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.clear();

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(1 /* array */, buf.position());
        }
    }

    /** */
    @Test
    public void testSerializationCopyConstructor() {
        MessageWriter writer = new DirectMessageWriter(null);

        ByteBuffer buf = ByteBuffer.allocate(4096);

        writer.setBuffer(buf);

        GridLongList ll = asList(1L, 2L, 3L);

        {
            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(25 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.add(2L);
            ll.add(4L);

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(41 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(33 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            for (int i = 0; i < 300; i++)
                ll.add(i);

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(2434 /* array */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.clear();

            Assert.assertTrue(writer.writeGridLongList(ll));

            Assert.assertEquals(1 /* array */, buf.position());
        }
    }

    /** */
    @Test
    public void testSerializationInsufficientBuffer() {
        MessageWriter writer = new DirectMessageWriter(null);

        ByteBuffer buf = ByteBuffer.allocate(10);

        writer.setBuffer(buf);

        GridLongList ll = asList(1L, 2L, 3L);

        Assert.assertFalse(writer.writeGridLongList(ll));

        Assert.assertEquals(10, buf.position());
    }

    /**
     * @param vals Values.
     * @return List from values.
     */
    private static GridLongList asList(long... vals) {
        if (F.isEmpty(vals))
            return new GridLongList();

        return new GridLongList(vals);
    }
}
