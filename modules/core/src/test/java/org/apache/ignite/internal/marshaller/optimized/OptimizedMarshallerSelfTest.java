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

package org.apache.ignite.internal.marshaller.optimized;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamField;
import java.io.Serializable;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * Optimized marshaller self test.
 */
@GridCommonTest(group = "Marshaller")
public class OptimizedMarshallerSelfTest extends GridCommonAbstractTest implements Serializable {
    private static Marshaller marsh;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        marsh = marshaller();
    }

    /** */
    protected Marshaller marshaller() {
        Marshaller marsh = new OptimizedMarshaller(false);

        marsh.setContext(new MarshallerContextTestImpl());

        return marsh;
    }

    /** Closure job. */
    protected IgniteInClosure<String> c1 = new IgniteInClosure<String>() {
        @Override public void apply(String s) {
            // No-op.
        }
    };

    /** Closure job. */
    protected IgniteClosure<String, String> c2 = new IgniteClosure<String, String>() {
        @Override public String apply(String s) {
            return s;
        }
    };

    /** Argument producer. */
    protected IgniteOutClosure<String> c3 = new IgniteOutClosure<String>() {
        @Nullable @Override public String apply() {
            return null;
        }
    };

    /** Reducer. */
    protected IgniteReducer<String, Object> c4 = new IgniteReducer<String, Object>() {
        @Override public boolean collect(String e) {
            return true;
        }

        @Nullable @Override public Object reduce() {
            return null;
        }
    };

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTestMarshalling() throws Exception {
        final String msg = "PASSED";

        byte[] buf = marsh.marshal(new IgniteRunnable() {
            @Override public void run() {
                c1.apply(msg);
                c2.apply(msg);

                c3.apply();
                c4.reduce();

                System.out.println("Test message: " + msg);
            }
        });

        Runnable r = marsh.unmarshal(buf, null);

        assertNotNull(r);

        r.run();
    }

    /**
     * Tests marshal self-linked object.
     *
     * @throws IgniteCheckedException If marshalling failed.
     */
    @Test
    public void testMarshallingSelfLink() throws IgniteCheckedException {
        SelfLink sl = new SelfLink("a string 1");

        sl.link(sl);

        SelfLink sl1 = marsh.unmarshal(marsh.marshal(sl), null);

        assert sl1.link() == sl1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvalid() throws Exception {
        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    byte[] arr = new byte[10];

                    arr[0] = (byte)200;

                    marsh.unmarshal(arr, null);

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
    @Test
    public void testNested() throws Exception {
        NestedTestObject obj = new NestedTestObject("String", 100);

        NestedTestObject newObj = marsh.unmarshal(marsh.marshal(obj), null);

        assertEquals("String", newObj.str);
        assertEquals(100, newObj.val);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimizedMarshaller() throws Exception {
        marsh.unmarshal(marsh.marshal(new TestClass()), null);

        TestClass2 val = marsh.unmarshal(marsh.marshal(new TestClass2()), null);

        assertNull(val.field3);
    }

    /**
     * Test class with serialPersistentFields fields.
     */
    private static class TestClass implements Serializable {
        private static final long serialVersionUID = 0L;

        /** For serialization compatibility. */
        private static final ObjectStreamField[] serialPersistentFields = {
            new ObjectStreamField("field1", Integer.TYPE),
            new ObjectStreamField("field2", Integer.TYPE)
        };

        /**
         * @param s Object output stream.
         */
        private void writeObject(ObjectOutputStream s) throws IOException {
            s.putFields().put("field1", 1);
            s.putFields().put("field2", 2);
            s.writeFields();

            s.writeObject(null);
        }

        /**
         * @param s Object input stream.
         */
        private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
            s.defaultReadObject();

            s.readObject();
        }
    }

    /**
     * Test class with serialPersistentFields fields.
     */
    private static class TestClass2 implements Serializable {
        private static final long serialVersionUID = 0L;

        /** */
        private Integer field3 = 1;

        /** For serialization compatibility. */
        private static final ObjectStreamField[] serialPersistentFields = {
            new ObjectStreamField("field1", Integer.TYPE),
            new ObjectStreamField("field2", Integer.TYPE)
        };

        /**
         * @param s Object output stream.
         */
        private void writeObject(ObjectOutputStream s) throws IOException {
            s.putFields().put("field1", 1);
            s.putFields().put("field2", 2);
            s.writeFields();

            s.writeObject(null);
        }

        /**
         * @param s Object input stream.
         */
        private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
            s.defaultReadObject();

            s.readObject();
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

        /** */
        private void writeObject(ObjectOutputStream out) throws IOException {
            try {
                byte[] arr = marsh.marshal(str);

                out.writeInt(arr.length);
                out.write(arr);

                out.writeInt(val);
            }
            catch (IgniteCheckedException e) {
                throw new IOException(e);
            }
        }

        /** */
        private void readObject(ObjectInputStream in) throws IOException {
            try {
                byte[] arr = new byte[in.readInt()];

                in.read(arr);

                str = marsh.unmarshal(arr, null);

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
}
