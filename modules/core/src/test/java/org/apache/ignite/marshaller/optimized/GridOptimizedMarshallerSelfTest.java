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

package org.apache.ignite.marshaller.optimized;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.gridgain.grid.marshaller.*;
import org.apache.ignite.internal.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * Optimized marshaller self test.
 */
@GridCommonTest(group = "Marshaller")
public class GridOptimizedMarshallerSelfTest extends GridMarshallerAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteMarshaller createMarshaller() {
        IgniteOptimizedMarshaller m = new IgniteOptimizedMarshaller();

        m.setRequireSerializable(false);
        m.setClassNames(F.asList(GoodMarshallable.class.getName(), NoMarshallable.class.getName()));

        return m;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTestMarshalling() throws Exception {
        final String msg = "PASSED";

        assert msg != null;

        byte[] buf = marshal(new IgniteRunnable() {
            @Override public void run() {
                c1.apply(msg);
                c2.apply(msg);

                c3.apply();
                c4.reduce();

                System.out.println("Test message: " + msg);
            }
        });

        Runnable r = unmarshal(buf);

        assertNotNull(r);

        r.run();
    }

    /**
     * Tests marshal self-linked object.
     *
     * @throws IgniteCheckedException If marshalling failed.
     */
    public void testMarshallingSelfLink() throws IgniteCheckedException {
        SelfLink sl = new SelfLink("a string 1");

        sl.link(sl);

        SelfLink sl1 = unmarshal(marshal(sl));

        assert sl1.link() == sl1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalid() throws Exception {
        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    unmarshal(new byte[10]);

                    return null;
                }
            },
            IgniteCheckedException.class,
            null
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testNested() throws Exception {
        NestedTestObject obj = new NestedTestObject("String", 100);

        NestedTestObject newObj = unmarshal(marshal(obj));

        assertEquals("String", newObj.str);
        assertEquals(100, newObj.val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGoodMarshallable() throws Exception {
        GoodMarshallable obj = new GoodMarshallable("String", 100);

        GoodMarshallable newObj = unmarshal(marshal(obj));

        assertEquals("String", newObj.getString());
        assertEquals(100, newObj.getInt());
    }

    /**
     * @throws Exception If failed.
     */
    public void testBadMarshallable() throws Exception {
        BadMarshallable obj = new BadMarshallable("String", 100);

        try {
            System.out.println(unmarshal(marshal(obj)));

            assert false;
        }
        catch (IgniteCheckedException e) {
            assert e.getCause() instanceof IOException;
            assert e.getCause().getMessage().contains("must return the value of the field");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void checkPerformance() throws Exception {
        final int cnt = 5_000_000;

        for (int j = 0; j < 5; j++) {
            long start = System.nanoTime();

            for (int i = 0; i < cnt; i++) {
                String s = "string-" + i;

                NoMarshallable obj = new NoMarshallable(s, i);

                NoMarshallable newObj = unmarshal(marshal(obj));

                assertEquals(s, newObj.getString());
            }

            X.println("Marshalling NoMarshallable duration: " + ((System.nanoTime() - start) / 1_000_000) + "ms");
        }


        for (int j = 0; j < 5; j++) {
            long start = System.nanoTime();

            for (int i = 0; i < cnt; i++) {
                String s = "string-" + i;

                GoodMarshallable obj = new GoodMarshallable(s, i);

                GoodMarshallable newObj = unmarshal(marshal(obj));

                assertEquals(s, newObj.getString());
            }

            X.println("Marshalling Marshallable duration: " + ((System.nanoTime() - start) / 1_000_000) + "ms");
        }
    }

    /**
     * Class for nested execution test.
     */
    private static class NestedTestObject implements Serializable {
        /** */
        private String str;

        /** */
        private int val;

        /**
         * @param str String.
         * @param val Value.
         */
        private NestedTestObject(String str, int val) {
            this.str = str;
            this.val = val;
        }

        /** {@inheritDoc} */
        private void writeObject(ObjectOutputStream out) throws IOException {
            try {
                byte[] arr = marshal(str);

                out.writeInt(arr.length);
                out.write(arr);

                out.writeInt(val);
            }
            catch (IgniteCheckedException e) {
                throw new IOException(e);
            }
        }

        /** {@inheritDoc} */
        @SuppressWarnings("UnusedParameters")
        private void readObject(ObjectInputStream in) throws IOException {
            try {
                byte[] arr = new byte[in.readInt()];

                in.read(arr);

                str = unmarshal(arr);

                val = in.readInt();
            }
            catch (IgniteCheckedException e) {
                throw new IOException(e);
            }
        }
    }

    /** */
    private static class TestObject2 {
        /** */
        private final int i;

        /**
         * Constructor for TestObject2 instances.
         *
         * @param i Integer value to hold.
         */
        private TestObject2(int i) {
            this.i = i;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return i == ((TestObject2)o).i;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return i;
        }
    }

    /**
     * Static nested class.
     */
    private static class TestObject {
        /** */
        private final TestObject2 o2;

        /** The only meaningful field in the class, used for {@link #equals(Object o)} and {@link #hashCode()}. */
        private final String str;

        /**
         * @param str String to hold.
         * @param i Integer.
         */
        TestObject(String str, int i) {
            this.str = str;

            o2 = new TestObject2(i);
        }

        /**
         * Method for accessing value of the hold string after the object is created.
         *
         * @return Wrapped string.
         */
        public String string() {
            return str;
        }

        /**
         * @return Object held in this wrapped.
         */
        public TestObject2 obj() {
            return o2;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 31 * o2.hashCode() + str.hashCode();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("RedundantIfStatement")
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestObject obj = (TestObject)o;

            if (o2 != null ? !o2.equals(obj.o2) : obj.o2 != null)
                return false;

            if (str != null ? !str.equals(obj.str) : obj.str != null)
                return false;

            return true;
        }
    }

    /**
     * Static nested class.
     */
    private static class SelfLink extends TestObject {
        /** */
        private SelfLink link;

        /**
         * @param str String to hold.
         */
        SelfLink(String str) {
            super(str, 1);
        }

        /**
         * @return The object this link points to,.
         */
        public SelfLink link() {
            return link;
        }

        /**
         * @param link The object this link should points to,
         */
        public void link(SelfLink link) {
            this.link = link;
        }
    }

    /**
     *
     */
    public static class GoodMarshallable implements IgniteOptimizedMarshallable, Serializable {
        /** Class ID required by {@link IgniteOptimizedMarshallable}. */
        @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
        private static Object GG_CLASS_ID;

        /** */
        private String str;

        /** */
        private int i;

        /**
         * @param str String.
         * @param i Integer.
         */
        public GoodMarshallable(String str, int i) {
            this.str = str;
            this.i = i;
        }

        /**
         * @return Int value.
         */
        private int getInt() {
            return i;
        }

        /**
         * @return String value
         */
        private String getString() {
            return str;
        }

        /** {@inheritDoc} */
        @Override public Object ggClassId() {
            return GG_CLASS_ID;
        }
    }

    /**
     *
     */
    public static class NoMarshallable implements Serializable {
        /** */
        private String str;

        /** */
        private int i;

        /**
         * @param str String.
         * @param i Integer.
         */
        public NoMarshallable(String str, int i) {
            this.str = str;
            this.i = i;
        }

        /**
         * @return Int value.
         */
        private int getInt() {
            return i;
        }

        /**
         * @return String value
         */
        private String getString() {
            return str;
        }
    }

    /**
     *
     */
    private static class BadMarshallable extends TestObject implements IgniteOptimizedMarshallable {
        /** Class ID required by {@link IgniteOptimizedMarshallable}. */
        @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
        private static Object GG_CLASS_ID;

        /**
         * @param str String.
         * @param i Integer.
         */
        private BadMarshallable(String str, int i) {
            super(str, i);
        }

        /** {@inheritDoc} */
        @Override public Object ggClassId() {
            return new Object();
        }
    }
}
