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

import java.io.IOException;
import java.net.ConnectException;
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Tests for {@link X}.
 */
@GridCommonTest(group = "Lang")
public class GridXSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testHasCause() {
        ConnectException conEx = new ConnectException();

        IOException ioEx = new IOException(conEx);

        IgniteCheckedException gridEx = new IgniteCheckedException(ioEx);

        assert X.hasCause(gridEx, IOException.class, NumberFormatException.class);
        assert !X.hasCause(gridEx, NumberFormatException.class);

        assert X.cause(gridEx, IOException.class) == ioEx;
        assert X.cause(gridEx, ConnectException.class) == conEx;
        assert X.cause(gridEx, NumberFormatException.class) == null;

        assert gridEx.getCause(IOException.class) == ioEx;
        assert gridEx.getCause(ConnectException.class) == conEx;
        assert gridEx.getCause(NumberFormatException.class) == null;
    }

    /**
     *
     */
    public void testShallowClone() {
        // Single not cloneable object
        Object obj = new Object();

        Object objClone = X.cloneObject(obj, false, true);

        assert objClone == obj;

        // Single cloneable object
        TestCloneable cloneable = new TestCloneable("Some string value");

        TestCloneable cloneableClone = X.cloneObject(cloneable, false, true);

        assert cloneableClone != null;
        assert cloneableClone != cloneable;
        assert cloneable.field.equals(cloneableClone.field);

        // Integer array.
        int[] intArr = {1, 2, 3};

        int[] intArrClone = X.cloneObject(intArr, false, true);

        assert intArrClone != null;
        assert intArrClone != intArr;
        assert Arrays.equals(intArrClone, intArr);

        // Boolean array.
        boolean[] boolArr = {true, false, true};

        boolean[] boolArrClone = X.cloneObject(boolArr, false, true);

        assert boolArrClone != null;
        assert boolArrClone != boolArr;
        assert Arrays.equals(boolArrClone, boolArr);

        // String array.
        String[] strArr = {"str1", "str2", "str3"};

        String[] strArrClone = X.cloneObject(strArr, false, true);

        assert strArrClone != null;
        assert strArrClone != strArr;
        assert Arrays.equals(strArrClone, strArr);
    }

    /**
     *
     */
    @SuppressWarnings({"StringEquality"})
    public void testDeepCloner() {
        // Single not cloneable object
        Object obj = new Object();

        Object objClone = X.cloneObject(obj, true, true);

        assert objClone != null;
        assert objClone != obj;

        // Single cloneable object
        TestCloneable cloneable = new TestCloneable("Some string value");

        TestCloneable cloneableClone = X.cloneObject(cloneable, true, false);

        assert cloneableClone != null;
        assert cloneableClone != cloneable;
        assert cloneable.field.equals(cloneableClone.field);
        assert cloneable.field != cloneableClone.field;

        // Single cloneable object
        TestCloneable1 cloneable1 = new TestCloneable1("Some string value");

        TestCloneable1 cloneableClone1 = X.cloneObject(cloneable1, true, false);

        assert cloneableClone1 != null;
        assert cloneableClone1 != cloneable1;
        assert cloneable1.field.equals(cloneableClone1.field);
        assert cloneable1.field != cloneableClone1.field;

        // Integer array.
        int[] intArr = {1, 2, 3};

        int[] intArrClone = X.cloneObject(intArr, true, false);

        assert intArrClone != null;
        assert intArrClone != intArr;
        assert Arrays.equals(intArrClone, intArr);

        // Boolean array.
        boolean[] boolArr = {true, false, true};

        boolean[] boolArrClone = X.cloneObject(boolArr, true, false);

        assert boolArrClone != null;
        assert boolArrClone != boolArr;
        assert Arrays.equals(boolArrClone, boolArr);

        // String array.
        String[] strArr = {"str1", "str2", "str3"};

        String[] strArrClone = X.cloneObject(strArr, true, false);

        assert strArrClone != null;
        assert strArrClone != strArr;
        assert Arrays.equals(strArrClone, strArr);

        for (int i = 0; i < strArr.length; i++) {
            assert strArr[i] != strArrClone[i];
            assert strArr[i].equals(strArrClone[i]);
        }

        // Cycles
        TestCycled testCycled = new TestCycled();
        TestCycled testCycledClone = X.cloneObject(testCycled, true, false);

        assert testCycledClone != null;
        assert testCycledClone != testCycled;
        assert testCycledClone == testCycledClone.cycle;

        // Cycles and hierarchy
        TestCycledChild testCycledChild = new TestCycledChild();
        TestCycledChild testCycledChildClone = X.cloneObject(testCycledChild, true, false);

        assert testCycledChildClone != null;
        assert testCycledChildClone != testCycledChild;
        assert testCycledChildClone == testCycledChildClone.cycle;
        assert testCycledChildClone == testCycledChildClone.anotherCycle;

        // Cloneable honored
        TestCloneable cloneable2 = new TestCloneable("Some string value");

        TestCloneable cloneableClone2 = X.cloneObject(cloneable2, true, true);

        assert cloneableClone2 != null;
        assert cloneableClone2 != cloneable2;
        assert cloneable2.field.equals(cloneableClone2.field);

        // Try clone class.
        X.cloneObject(Integer.class, true, true);
    }

    /**
     * Test cloneable class.
     */
    private static class TestCloneable implements Cloneable {
        /** */
        private String field;

        /** */
        @SuppressWarnings({"unused"})
        private String field1;

        /** */
        @SuppressWarnings({"unused"})
        private final Class cls = Integer.class;

        /**
         * @param val Field value.
         */
        private TestCloneable(String val) {
            field = val;
        }

        /** {@inheritDoc} */
        @Override protected Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    /**
     * Test cloneable class.
     */
    private static class TestCloneable1 {
        /** */
        private String field;

        /**
         * @param val Field value.
         */
        private TestCloneable1(String val) {
            field = val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return field.hashCode();
        }
    }

    /**
     * Class to test deep cloning with cycles.
     */
    private static class TestCycled {
        /** */
        protected final TestCycled cycle = this;
    }

    /**
     * Class to test hierarchy init.
     */
    private static class TestCycledChild extends TestCycled {
        /** */
        @SuppressWarnings({"unused"})
        private final TestCycled anotherCycle = this;
    }
}