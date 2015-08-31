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

package org.apache.ignite.marshaller;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.internal.util.typedef.COX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Marshallers benchmark.
 */
public class GridMarshallerPerformanceTest extends GridCommonAbstractTest {
    /** Number of iterations per test. */
    private static final int ITER_CNT = 1 * 1000 * 1000;

    /**
     * Flag that sets whether collections should be marshalled and
     * unmarshalled using writeObject/readObject methods.
     *
     * Set to false to correctly compare with ByteBuffer.
     */
    private static final boolean MARSHAL_COLS_AS_OBJECTS = false;

    /**
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        IgniteInClosure<TestObject> writer = new CIX1<TestObject>() {
            @Override public void applyx(TestObject obj) throws IgniteCheckedException {
                out.reset();

                ObjectOutputStream objOut = null;

                try {
                    objOut = new ObjectOutputStream(out);

                    objOut.writeObject(obj);
                }
                catch (IOException e) {
                    throw new IgniteCheckedException(e);
                }
                finally {
                    U.close(objOut, log);
                }
            }
        };

        IgniteOutClosure<TestObject> reader = new COX<TestObject>() {
            @Override public TestObject applyx() throws IgniteCheckedException {
                ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

                ObjectInputStream objIn = null;

                try {
                    objIn = new ObjectInputStream(in);

                    return (TestObject)objIn.readObject();
                }
                catch (ClassNotFoundException | IOException e) {
                    throw new IgniteCheckedException(e);
                } finally {
                    U.close(objIn, log);
                }
            }
        };

        runTest("Serialization", writer, reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridMarshaller() throws Exception {
        final GridTuple<byte[]> tuple = new GridTuple<>();

        final OptimizedMarshaller marsh = new OptimizedMarshaller();

        IgniteInClosure<TestObject> writer = new CIX1<TestObject>() {
            @Override public void applyx(TestObject obj) throws IgniteCheckedException {
                tuple.set(marsh.marshal(obj));
            }
        };

        IgniteOutClosure<TestObject> reader = new COX<TestObject>() {
            @Override public TestObject applyx() throws IgniteCheckedException {
                return marsh.unmarshal(tuple.get(), null);
            }
        };

        runTest("GridMarshaller", writer, reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testByteBuffer() throws Exception {
        final ByteBuffer buf = ByteBuffer.allocate(1024);

        IgniteInClosure<TestObject> writer = new CI1<TestObject>() {
            @Override public void apply(TestObject obj) {
                buf.clear();

                obj.write(buf);
            }
        };

        IgniteOutClosure<TestObject> reader = new CO<TestObject>() {
            @Override public TestObject apply() {
                buf.flip();

                return TestObject.read(buf);
            }
        };

        runTest("ByteBuffer", writer, reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testKryo() throws Exception {
        final Kryo kryo = new Kryo();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        IgniteInClosure<TestObject> writer = new CI1<TestObject>() {
            @Override public void apply(TestObject obj) {
                out.reset();

                Output kryoOut = null;

                try {
                    kryoOut = new Output(out);

                    kryo.writeObject(kryoOut, obj);
                }
                finally {
                    U.close(kryoOut, log);
                }
            }
        };

        IgniteOutClosure<TestObject> reader = new CO<TestObject>() {
            @Override public TestObject apply() {
                Input kryoIn = null;

                try {
                    kryoIn = new Input(new ByteArrayInputStream(out.toByteArray()));

                    return kryo.readObject(kryoIn, TestObject.class);
                }
                finally {
                    U.close(kryoIn, log);
                }
            }
        };

        runTest("Kryo", writer, reader);
    }

    /**
     * @param name Test name.
     * @param writer Writer closure.
     * @param reader Reader closure.
     * @throws Exception In case of error.
     */
    private void runTest(String name, IgniteInClosure<TestObject> writer,
        IgniteOutClosure<TestObject> reader) throws Exception {
        ArrayList<Float> list = new ArrayList<>();

        list.add(10.0f);
        list.add(20.0f);
        list.add(30.0f);
        list.add(40.0f);
        list.add(50.0f);

        HashMap<Integer, Character> map = new HashMap<>();

        map.put(1, 'a');
        map.put(2, 'b');
        map.put(3, 'c');
        map.put(4, 'd');
        map.put(5, 'e');

        TestObject obj = new TestObject(123, 1234L, true,
            new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            new double[] {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0},
            list, map);

        for (int i = 0; i < 3; i++) {
            long start = System.currentTimeMillis();

            for (int j = 0; j < ITER_CNT; j++) {
                writer.apply(obj);

                TestObject newObj = reader.apply();

                assertEquals(obj, newObj);
            }

            long duration = System.currentTimeMillis() - start;

            System.out.format("%d %s => %,d ms\n", i, name, duration);
        }
    }

    /**
     * Test object.
     */
    private static class TestObject implements Externalizable, KryoSerializable {
        /** Integer value. */
        private int intVal;

        /** Long value. */
        private long longVal;

        /** Boolean value. */
        private boolean boolVal;

        /** Array of longs. */
        private long[] longArr;

        /** Array of doubles. */
        private double[] dblArr;

        /** ArrayList. */
        private ArrayList<Float> list;

        /** HashMap. */
        private HashMap<Integer, Character> map;

        /** Self reference. */
        private TestObject selfRef;

        /**
         * Required by {@link Externalizable}.
         */
        public TestObject() {
            // No-op.
        }

        /**
         * @param intVal Integer value.
         * @param longVal Long value.
         * @param boolVal Boolean value.
         * @param longArr Array of longs.
         * @param dblArr Array of doubles.
         * @param list Collection.
         * @param map Map.
         */
        TestObject(int intVal, long longVal, boolean boolVal, long[] longArr, double[] dblArr,
            ArrayList<Float> list, HashMap<Integer, Character> map) {
            this.intVal = intVal;
            this.longVal = longVal;
            this.boolVal = boolVal;
            this.longArr = longArr;
            this.dblArr = dblArr;
            this.list = list;
            this.map = map;

            selfRef = this;
        }

        /**
         * Writes this object to {@link ByteBuffer}.
         *
         * @param buf Buffer.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        void write(ByteBuffer buf) {
            buf.putInt(intVal);
            buf.putLong(longVal);
            buf.put((byte)(boolVal ? 1 : 0));

            buf.putInt(longArr.length);

            for (long l : longArr)
                buf.putLong(l);

            buf.putInt(dblArr.length);

            for (double d : dblArr)
                buf.putDouble(d);

            buf.putInt(list.size());

            for (int i = 0; i < list.size(); i++)
                buf.putFloat(list.get(i));

            buf.putInt(map.size());

            for (Map.Entry<Integer, Character> e : map.entrySet()) {
                buf.putInt(e.getKey());
                buf.putChar(e.getValue());
            }
        }

        /**
         * Reads from {@link ByteBuffer}.
         *
         * @param buf Buffer.
         * @return Object.
         */
        static TestObject read(ByteBuffer buf) {
            int intVal = buf.getInt();
            long longVal = buf.getLong();
            boolean boolVal = buf.get() == 1;

            int longArrSize = buf.getInt();

            long[] longArr = new long[longArrSize];

            for (int i = 0; i < longArrSize; i++)
                longArr[i] = buf.getLong();

            int dblArrSize = buf.getInt();

            double[] dblArr = new double[dblArrSize];

            for (int i = 0; i < dblArrSize; i++)
                dblArr[i] = buf.getDouble();

            int listSize = buf.getInt();

            ArrayList<Float> list = new ArrayList<>(listSize);

            for (int i = 0; i < listSize; i++)
                list.add(buf.getFloat());

            int mapSize = buf.getInt();

            HashMap<Integer, Character> map = new HashMap<>(mapSize);

            for (int i = 0; i < mapSize; i++)
                map.put(buf.getInt(), buf.getChar());

            TestObject o = new TestObject(intVal, longVal, boolVal, longArr, dblArr, list, map);

            o.selfRef = o;

            return o;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(intVal);
            out.writeLong(longVal);
            out.writeBoolean(boolVal);
            out.writeObject(longArr);
            out.writeObject(dblArr);

            if (MARSHAL_COLS_AS_OBJECTS) {
                out.writeObject(list);
                out.writeObject(map);
            }
            else {
                out.writeInt(list.size());

                for (int i = 0; i < list.size(); i++)
                    out.writeFloat(list.get(i));

                out.writeInt(map.size());

                for (Map.Entry<Integer, Character> e : map.entrySet()) {
                    out.writeInt(e.getKey());
                    out.writeChar(e.getValue());
                }
            }

            out.writeObject(selfRef);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            intVal = in.readInt();
            longVal = in.readLong();
            boolVal = in.readBoolean();
            longArr = (long[])in.readObject();
            dblArr = (double[])in.readObject();

            if (MARSHAL_COLS_AS_OBJECTS) {
                list = (ArrayList<Float>)in.readObject();
                map = (HashMap<Integer, Character>)in.readObject();
            }
            else {
                int listSize = in.readInt();

                list = new ArrayList<>(listSize);

                for (int i = 0; i < listSize; i++)
                    list.add(in.readFloat());

                int mapSize = in.readInt();

                map = new HashMap<>(mapSize);

                for (int i = 0; i < mapSize; i++)
                    map.put(in.readInt(), in.readChar());
            }

            selfRef = (TestObject)in.readObject();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        @Override public void write(Kryo kryo, Output out) {
            kryo.writeObject(out, intVal);
            kryo.writeObject(out, longVal);
            kryo.writeObject(out, boolVal);
            kryo.writeObject(out, longArr);
            kryo.writeObject(out, dblArr);

            if (MARSHAL_COLS_AS_OBJECTS) {
                kryo.writeObject(out, list);
                kryo.writeObject(out, map);
            }
            else {
                kryo.writeObject(out, list.size());

                for (int i = 0; i < list.size(); i++)
                    kryo.writeObject(out, list.get(i));

                kryo.writeObject(out, map.size());

                for (Map.Entry<Integer, Character> e : map.entrySet()) {
                    kryo.writeObject(out, e.getKey());
                    kryo.writeObject(out, e.getValue());
                }
            }

            kryo.writeObject(out, selfRef);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void read(Kryo kryo, Input in) {
            intVal = kryo.readObject(in, Integer.class);
            longVal = kryo.readObject(in, Long.class);
            boolVal = kryo.readObject(in, Boolean.class);
            longArr = kryo.readObject(in, long[].class);
            dblArr = kryo.readObject(in, double[].class);

            if (MARSHAL_COLS_AS_OBJECTS) {
                list = kryo.readObject(in, ArrayList.class);
                map = kryo.readObject(in, HashMap.class);
            }
            else {
                int listSize = kryo.readObject(in, Integer.class);

                list = new ArrayList<>(listSize);

                for (int i = 0; i < listSize; i++)
                    list.add(kryo.readObject(in, Float.class));

                int mapSize = kryo.readObject(in, Integer.class);

                map = new HashMap<>(mapSize);

                for (int i = 0; i < mapSize; i++)
                    map.put(kryo.readObject(in, Integer.class), kryo.readObject(in, Character.class));
            }

            selfRef = kryo.readObject(in, TestObject.class);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object other) {
            if (this == other)
                return true;

            if (other == null || getClass() != other.getClass())
                return false;

            TestObject obj = (TestObject)other;

            assert this == selfRef;
            assert obj == obj.selfRef;

            return boolVal == obj.boolVal && intVal == obj.intVal && longVal == obj.longVal &&
                Arrays.equals(dblArr, obj.dblArr) && Arrays.equals(longArr, obj.longArr) &&
                list.equals(obj.list) && map.equals(obj.map);
        }
    }
}