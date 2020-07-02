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

package org.apache.ignite.internal.binary;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

/**
 * Array identity resolver self test.
 */
public class BinaryArrayIdentityResolverSelfTest extends GridCommonAbstractTest {
    /** Pointers to release. */
    private final Set<Long> ptrs = new GridConcurrentHashSet<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (Long ptr : ptrs)
            GridUnsafe.freeMemory(ptr);

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(new BinaryMarshaller());

        BinaryConfiguration binCfg = new BinaryConfiguration();

        BinaryTypeConfiguration binTypCfg1 = new BinaryTypeConfiguration();
        BinaryTypeConfiguration binTypCfg2 = new BinaryTypeConfiguration();

        binTypCfg1.setTypeName(InnerClass.class.getName());
        binTypCfg2.setTypeName(InnerClassBinarylizable.class.getName());

        List<BinaryTypeConfiguration> binTypCfgs = new ArrayList<>();

        binTypCfgs.add(binTypCfg1);
        binTypCfgs.add(binTypCfg2);

        binCfg.setTypeConfigurations(binTypCfgs);

        cfg.setBinaryConfiguration(binCfg);

        return cfg;
    }

    /**
     * Test hash code generation for simple object.
     */
    @Test
    public void testHashCode() {
        InnerClass obj = new InnerClass(1, "2", 3);

        int expHash = BinaryArrayIdentityResolver.instance().hashCode(asBinary(obj));

        assertEquals(expHash, build(InnerClass.class, "a", obj.a, "b", obj.b, "c", obj.c).hashCode());
    }

    /**
     * Test hash code generation for simple object.
     */
    @Test
    public void testHashCodeBinarylizable() {
        InnerClassBinarylizable obj = new InnerClassBinarylizable(1, "2", 3);

        int expHash = BinaryArrayIdentityResolver.instance().hashCode(asBinary(obj));

        assertEquals(expHash, build(InnerClass.class, "a", obj.a, "b", obj.b, "c", obj.c).hashCode());
    }

    /**
     * Test equals for simple object.
     */
    @Test
    public void testEquals() {
        InnerClass obj = new InnerClass(1, "2", 3);

        // Positive cases.
        compareTwo(asBinary(obj), asBinary(obj), true);
        compareTwo(asBinary(obj), build(InnerClass.class, "a", obj.a, "b", obj.b, "c", obj.c), true);

        // Negative cases.
        compareTwo(asBinary(obj), build(InnerClass.class, "a", obj.a, "b", obj.b), false);
        compareTwo(asBinary(obj), build(InnerClass.class, "a", obj.a, "b", obj.b, "c", obj.c, "d", "d"), false);
        compareTwo(asBinary(obj), build(InnerClass.class, "a", obj.a), false);
        compareTwo(asBinary(obj), build(InnerClass.class, "a", obj.a, "b", obj.b + "1"), false);
        compareTwo(asBinary(obj), build(InnerClass.class, "a", obj.a + 1, "b", obj.b), false);
    }

    /**
     * Test equals for simple object.
     */
    @Test
    public void testEqualsBinarilyzable() {
        InnerClassBinarylizable obj = new InnerClassBinarylizable(1, "2", 3);

        // Positive cases.
        compareTwo(asBinary(obj), asBinary(obj), true);
        compareTwo(asBinary(obj), build(InnerClassBinarylizable.class, "a", obj.a, "b", obj.b, "c", obj.c),
            true);

        // Negative cases.
        compareTwo(asBinary(obj), build(InnerClassBinarylizable.class, "a", obj.a, "b", obj.b), false);
        compareTwo(asBinary(obj), build(InnerClassBinarylizable.class, "a", obj.a, "b", obj.b, "c", obj.c, "d", "d"),
            false);
        compareTwo(asBinary(obj), build(InnerClassBinarylizable.class, "a", obj.a), false);
        compareTwo(asBinary(obj), build(InnerClassBinarylizable.class, "a", obj.a, "b", obj.b + "1"), false);
        compareTwo(asBinary(obj), build(InnerClassBinarylizable.class, "a", obj.a + 1, "b", obj.b), false);
    }

    /**
     * Test equals for different type IDs.
     */
    @Test
    public void testEqualsDifferenTypes() {
        InnerClass obj1 = new InnerClass(1, "2", 3);
        InnerClassBinarylizable obj2 = new InnerClassBinarylizable(1, "2", 3);

        compareTwo(asBinary(obj1), asBinary(obj2), false);
    }

    /**
     * Compare two objects in different heap/offheap modes.
     *
     * @param obj1 Object 1.
     * @param obj2 Object 2.
     * @param expRes Expected result.
     */
    private void compareTwo(BinaryObject obj1, BinaryObject obj2, boolean expRes) {
        if (expRes) {
            assertEquals(convert(obj1, false), convert(obj2, false));
            assertEquals(convert(obj1, false), convert(obj2, true));
            assertEquals(convert(obj1, true), convert(obj2, false));
            assertEquals(convert(obj1, true), convert(obj2, true));
        }
        else {
            assertNotEquals(convert(obj1, false), convert(obj2, false));
            assertNotEquals(convert(obj1, false), convert(obj2, true));
            assertNotEquals(convert(obj1, true), convert(obj2, false));
            assertNotEquals(convert(obj1, true), convert(obj2, true));
        }
    }

    /**
     * Convert to binary object.
     *
     * @param obj Original object.
     * @return Binary object.
     */
    private BinaryObject asBinary(Object obj) {
        return grid().binary().toBinary(obj);
    }

    /**
     * Build object of the given type with provided fields.
     *
     * @param cls Class.
     * @param parts Parts.
     * @return Result.
     */
    private BinaryObject build(Class cls, Object... parts) {
        BinaryObjectBuilder builder = grid().binary().builder(cls.getName());

        if (!F.isEmpty(parts)) {
            for (int i = 0; i < parts.length; )
                builder.setField((String)parts[i++], parts[i++]);
        }

        return builder.build();
    }

    /**
     * Inner class.
     */
    private static class InnerClass {
        /** Field a. */
        public int a;

        /** Field b. */
        public String b;

        /** Field c. */
        public long c;

        /**
         * Constructor.
         *
         * @param a Field a.
         * @param b Field b.
         * @param c Field c.
         */
        public InnerClass(int a, String b, long c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }
    }

    /**
     * Convert binary object to it's final state.
     *
     * @param obj Object.
     * @param offheap Offheap flag.
     * @return Result.
     */
    private BinaryObjectExImpl convert(BinaryObject obj, boolean offheap) {
        BinaryObjectExImpl obj0 = (BinaryObjectExImpl)obj;

        if (offheap) {
            byte[] arr = obj0.array();

            long ptr = GridUnsafe.allocateMemory(arr.length);

            ptrs.add(ptr);

            GridUnsafe.copyMemory(arr, GridUnsafe.BYTE_ARR_OFF, null, ptr, arr.length);

            obj0 = new BinaryObjectOffheapImpl(obj0.context(), ptr, 0, obj0.array().length);
        }

        return obj0;
    }

    /**
     * Inner class with Binarylizable interface.
     */
    private static class InnerClassBinarylizable extends InnerClass implements Binarylizable {
        /**
         * Constructor.
         *
         * @param a Field a.
         * @param b Field b.
         * @param c Field c.
         */
        public InnerClassBinarylizable(int a, String b, long c) {
            super(a, b, c);
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("a", a);
            writer.writeString("b", b);
            writer.writeLong("c", c);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            a = reader.readInt("a");
            b = reader.readString("b");
            c = reader.readLong("c");
        }
    }
}
