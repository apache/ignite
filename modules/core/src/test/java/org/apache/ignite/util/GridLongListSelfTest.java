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
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.internal.util.GridLongList.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class GridLongListSelfTest {
    /** Size of the header when list is written with message writer. */
    private static final int HEADER_SIZE = 2;

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    @Test
    public void testCopyWithout() throws Exception {
        assertCopy(
            new GridLongList(new long[] {}),
            new GridLongList(new long[] {}));

        assertCopy(
            new GridLongList(new long[] {}),
            new GridLongList(new long[] {1}));

        assertCopy(
            new GridLongList(new long[] {1}),
            new GridLongList(new long[] {}));

        assertCopy(
            new GridLongList(new long[] {1, 2, 3}),
            new GridLongList(new long[] {4, 5, 6}));

        assertCopy(
            new GridLongList(new long[] {1, 2, 3}),
            new GridLongList(new long[] {1, 2, 3}));

        assertCopy(
            new GridLongList(new long[] {1, 2, 3, 4, 5, 1}),
            new GridLongList(new long[] {1, 1}));

        assertCopy(
            new GridLongList(new long[] {1, 1, 1, 2, 3, 4, 5, 1, 1, 1}),
            new GridLongList(new long[] {1, 1}));

        assertCopy(
            new GridLongList(new long[] {1, 2, 3}),
            new GridLongList(new long[] {1, 1, 2, 2, 3, 3}));
    }

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
     * Assert {@link GridLongList#copyWithout(GridLongList)} on given lists.
     *
     * @param lst Source lists.
     * @param rmv Exclude list.
     */
    private void assertCopy(GridLongList lst, GridLongList rmv) {
        GridLongList res = lst.copyWithout(rmv);

        for (int i = 0; i < lst.size(); i++) {
            long v = lst.get(i);

            if (rmv.contains(v))
                assertFalse(res.contains(v));
            else
                assertTrue(res.contains(v));
        }
    }

    /**
     *
     */
    @Test
    public void testRemove() {
        GridLongList list = asList(1,2,3,4,5,6);

        assertEquals(2, list.removeValue(0, 3));
        assertEquals(asList(1,2,4,5,6), list);

        assertEquals(-1, list.removeValue(1, 1));
        assertEquals(-1, list.removeValue(0, 3));

        assertEquals(4, list.removeValue(0, 6));
        assertEquals(asList(1,2,4,5), list);

        assertEquals(2, list.removeIndex(1));
        assertEquals(asList(1,4,5), list);

        assertEquals(1, list.removeIndex(0));
        assertEquals(asList(4,5), list);
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
    public void testArray() {
        GridLongList list = new GridLongList();

        long[] array = list.array();

        assertNotNull(array);

        assertEquals(0, array.length);

        list.add(1L);

        array = list.array();

        assertNotNull(array);

        assertEquals(1, array.length);

        assertEquals(1L, array[0]);
    }

    /** */
    @Test
    public void testSerializationDefaultConstructor() {
        MessageWriter writer = new DirectMessageWriter(GridIoManager.DIRECT_PROTO_VER);

        ByteBuffer buf = ByteBuffer.allocate(4096);

        GridLongList ll = new GridLongList();

        {
            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 1 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.add(2L);
            ll.add(4L);

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 17 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 9 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 1 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            for (int i = 0; i < 300; i++)
                ll.add(i);

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(300, ll.size());

            Assert.assertEquals(HEADER_SIZE + 2402 /* array */ + 2 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.clear();

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 1 /* array */ + 1 /* index */, buf.position());
        }
    }

    /** */
    @Test
    public void testSerializationConstructorWithSize() {
        MessageWriter writer = new DirectMessageWriter(GridIoManager.DIRECT_PROTO_VER);

        ByteBuffer buf = ByteBuffer.allocate(4096);

        GridLongList ll = new GridLongList(5);

        {
            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 1 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.add(2L);
            ll.add(4L);

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 17 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 9 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 1 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            for (int i = 0; i < 300; i++)
                ll.add(i);

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(300, ll.size());

            Assert.assertEquals(HEADER_SIZE + 2402 /* array */ + 2 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.clear();

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 1 /* array */ + 1 /* index */, buf.position());
        }
    }

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12678")
    @Test
    public void testSerializationConstructorWithZeroSize() {
        MessageWriter writer = new DirectMessageWriter(GridIoManager.DIRECT_PROTO_VER);

        ByteBuffer buf = ByteBuffer.allocate(4096);

        GridLongList ll = new GridLongList(0);

        {
            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 1 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.add(2L);
            ll.add(4L);

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 17 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 9 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 1 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            for (int i = 0; i < 300; i++)
                ll.add(i);

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(300, ll.size());

            Assert.assertEquals(HEADER_SIZE + 2402 /* array */ + 2 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.clear();

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 1 /* array */ + 1 /* index */, buf.position());
        }
    }

    /** */
    @Test
    public void testSerializationCopyConstructor() {
        MessageWriter writer = new DirectMessageWriter(GridIoManager.DIRECT_PROTO_VER);

        ByteBuffer buf = ByteBuffer.allocate(4096);

        GridLongList ll = new GridLongList(new long[]{1L, 2L, 3L});

        {
            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 25 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.add(2L);
            ll.add(4L);

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 41 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.remove();

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 33 /* array */ + 1 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            for (int i = 0; i < 300; i++)
                ll.add(i);

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 2434 /* array */ + 2 /* index */, buf.position());
        }

        {
            writer.reset();
            buf.clear();

            ll.clear();

            Assert.assertTrue(ll.writeTo(buf, writer));

            Assert.assertEquals(HEADER_SIZE + 1 /* array */ + 1 /* index */, buf.position());
        }
    }

    /** */
    @Test
    public void testSerializationInsufficientBuffer() {
        MessageWriter writer = new DirectMessageWriter(GridIoManager.DIRECT_PROTO_VER);

        ByteBuffer buf = ByteBuffer.allocate(10);

        GridLongList ll = new GridLongList(new long[]{1L, 2L, 3L});

        Assert.assertFalse(ll.writeTo(buf, writer));

        Assert.assertEquals(10, buf.position());
    }
}
