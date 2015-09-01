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

package org.apache.ignite.lang;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Tests for {@link org.apache.ignite.lang.IgniteUuid}.
 */
@GridCommonTest(group = "Lang")
public class IgniteUuidSelfTest extends GridCommonAbstractTest {
    /** Sample size. */
    private static final int NUM = 100000;

    /**
     * JUnit.
     */
    public void testToString() {
        IgniteUuid id1 = IgniteUuid.randomUuid();

        String id8_1 = U.id8(id1);

        info("ID1: " + id1);
        info("ID8_1: " + id8_1);

        String s = id1.toString();

        IgniteUuid id2 = IgniteUuid.fromString(s);

        String id8_2 = U.id8(id2);

        info("ID2: " + id2);
        info("ID8_2: " + id8_2);

        assertNotNull(id2);
        assertNotNull(id8_1);
        assertNotNull(id8_2);

        assertEquals(8, id8_1.length());
        assertEquals(8, id8_2.length());

        assertEquals(id1, id2);
        assertEquals(id8_1, id8_2);
    }

    /**
     * JUnit.
     */
    public void testGridUuid() {
        IgniteUuid id1 = IgniteUuid.randomUuid();
        IgniteUuid id2 = IgniteUuid.randomUuid();

        assert id1.compareTo(id2) == -1;
        assert id2.compareTo(id1) == 1;
        assert id1.compareTo(id1) == 0;
        assert id2.compareTo(id2) == 0;

        assert id1.hashCode() != id2.hashCode();
        assert !id1.equals(id2);

        assert id1.iterator().hasNext();
        assert id1.iterator().next().equals(id1);

        assert id2.iterator().hasNext();
        assert id2.iterator().next().equals(id2);
    }

    /**
     * JUnit.
     */
    public void testGridUuidPerformance() {
        long start = System.currentTimeMillis();

        for (int i = 0; i < NUM; i++)
            UUID.randomUUID();

        long dur1 = System.currentTimeMillis() - start;

        start = System.currentTimeMillis();

        for (int i = 0; i < NUM; i++)
            IgniteUuid.randomUuid();

        long dur2 = System.currentTimeMillis() - start;

        if (dur2 == 0)
            dur2 = 1;

        System.out.println("Creation: UUID=" + dur1 + "ms, GridUuid=" + dur2 + "ms, " + (dur1 / dur2) + "x faster.");
    }

    /**
     * Tests serialization performance.
     *
     * @throws Exception If failed.
     */
    public void testSerializationPerformance() throws Exception {
        UuidBean[] uids = new UuidBean[NUM];

        // Populate.
        for (int i = 0; i < uids.length; i++)
            uids[i] = new UuidBean(UUID.randomUUID());

        GridUuidBean[] guids = new GridUuidBean[NUM];

        // Populate.
        for (int i = 0; i < guids.length; i++)
            guids[i] = new GridUuidBean(IgniteUuid.randomUuid());

        // Warm up.
        testArray(uids, NUM);
        testArray(guids, NUM);

        System.gc();

        long start = System.currentTimeMillis();

        testArray(uids, NUM);

        long dur1 = System.currentTimeMillis() - start;

        System.gc();

        start = System.currentTimeMillis();

        testArray(guids, NUM);

        long dur2 = System.currentTimeMillis() - start;

        long res = dur1 < dur2 ? (dur2 / dur1) : (dur1 / dur2);

        String metric = res == 1 ? "as fast as UUID" : dur1 < dur2 ? res + "x **SLOWER**" : res + "x faster";

        System.out.println("Serialization: UUID=" + dur1 + "ms, GridUuid=" + dur2 + "ms, GridUuid is " +
            metric + ".");
    }

    /**
     * @param objs Objects to test.
     * @param cnt Number of iterations.
     * @throws Exception If failed.
     */
    private void testArray(Object[] objs, int cnt) throws Exception {
        assert objs.length > 0;

        int size = 0;

        for (int i = 0; i < cnt; i++) {
            Object w = objs[i];

            byte[] bytes = write(w);

            size += bytes.length;

            Object r = read(bytes);

            assert r != null;
            assert r.equals(w);
        }

        info("Average byte size of serialized object [size=" + (size / cnt) +
            ", type=" + objs[0].getClass().getSimpleName() + ']');
    }

    /**
     * @param uid UUID.
     * @return Serialized bytes.
     * @throws IOException If failed.
     */
    private byte[] write(Object uid) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream(256);

        ObjectOutputStream out = null;

        try {
            out = new ObjectOutputStream(bout);

            out.writeObject(uid);

            return bout.toByteArray();
        }
        finally {
            if (out != null)
                out.close();
        }
    }

    /**
     * @param bytes Serialized bytes.
     * @return Deserialized object.
     * @throws ClassNotFoundException If failed.
     * @throws IOException If failed.
     */
    @SuppressWarnings({"unchecked"})
    private <T> T read(byte[] bytes) throws ClassNotFoundException, IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);

        ObjectInputStream in = null;

        try {
            in = new ObjectInputStream(bin);

            return (T)in.readObject();
        }
        finally {
            if (in != null)
                in.close();
        }
    }

    /**
     *
     */
    private static class GridUuidBean implements Externalizable {
        /** */
        private IgniteUuid uid;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public GridUuidBean() {
            // No-op.
        }

        /**
         * @param uid Grid UUID.
         */
        private GridUuidBean(IgniteUuid uid) {
            this.uid = uid;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            // Uncomment if you would like to see raw serialization performance.
            //out.writeObject(uid);

            U.writeGridUuid(out, uid);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // Uncomment if you would like to see raw serialization performance.
            //uid = (GridUuid)in.readObject();

            uid = U.readGridUuid(in);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || uid.equals(((GridUuidBean)o).uid);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return uid.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() { return S.toString(GridUuidBean.class, this); }
    }

    /**
     *
     */
    private static class UuidBean implements Externalizable {
        /** */
        private UUID uid;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public UuidBean() {
            // No-op.
        }

        /**
         * @param uid Grid UUID.
         */
        private UuidBean(UUID uid) {
            this.uid = uid;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeUuid(out, uid);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            uid = U.readUuid(in);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || uid.equals(((UuidBean)o).uid);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return uid.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() { return S.toString(UuidBean.class, this); }
    }
}