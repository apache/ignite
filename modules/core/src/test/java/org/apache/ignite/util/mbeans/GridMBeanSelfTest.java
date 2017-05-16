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

package org.apache.ignite.util.mbeans;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.StandardMBean;
import org.apache.ignite.internal.mxbean.IgniteStandardMXBean;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.mxbean.MXBeanParametersDescriptions;
import org.apache.ignite.mxbean.MXBeanParametersNames;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * MBean test.
 */
public class GridMBeanSelfTest extends GridCommonAbstractTest {
    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    public void testCorrectMBeanInfo() throws Exception {
        StandardMBean mbean = new IgniteStandardMXBean(new GridMBeanImplementation(), GridMBeanInterface.class);

        MBeanInfo info = mbean.getMBeanInfo();

        assert info.getDescription().equals("MBeanDescription.") == true;

        assert info.getOperations().length == 2;

        for (MBeanOperationInfo opInfo : info.getOperations()) {
            if (opInfo.getDescription().equals("MBeanOperation."))
                assert opInfo.getSignature().length == 2;
            else {
                assert opInfo.getDescription().equals("MBeanSuperOperation.") == true;
                assert opInfo.getSignature().length == 1;
            }
        }

        for (MBeanParameterInfo paramInfo : info.getOperations()[0].getSignature()) {
            if (paramInfo.getName().equals("ignored"))
                assert paramInfo.getDescription().equals("MBeanOperationParameter1.") == true;
            else {
                assert paramInfo.getName().equals("someData") == true;
                assert paramInfo.getDescription().equals("MBeanOperationParameter2.") == true;
            }
        }

        assert info.getAttributes().length == 4 : "Expected 4 attributes but got " + info.getAttributes().length;

        for (MBeanAttributeInfo attrInfo : info.getAttributes()) {
            if (attrInfo.isWritable() == false) {
                assert (attrInfo.getDescription().equals("MBeanReadonlyGetter.") == true ||
                    attrInfo.getDescription().equals("MBeanROGetter."));
            }
            else {
                assert (attrInfo.getDescription().equals("MBeanWritableGetter.") == true ||
                    attrInfo.getDescription().equals("MBeanWritableIsGetter."));
            }
        }
    }

    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    public void testMissedNameMBeanInfo() throws Exception {
        try {
            StandardMBean mbean = new IgniteStandardMXBean(new GridMBeanImplementation(), GridMBeanInterfaceBad.class);

            mbean.getMBeanInfo();
        }
        catch (AssertionError ignored) {
            return;
        }

        assert false;
    }

    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    public void testMissedDescriptionMBeanInfo() throws Exception {
        try {
            StandardMBean mbean = new IgniteStandardMXBean(new GridMBeanImplementation(),
                GridMBeanInterfaceBadAgain.class);

            mbean.getMBeanInfo();
        }
        catch (AssertionError ignored) {
            return;
        }

        assert false;
    }

    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    public void testEmptyDescriptionMBeanInfo() throws Exception {
        try {
            StandardMBean mbean = new IgniteStandardMXBean(new GridMBeanImplementation(),
                GridMBeanInterfaceEmptyDescription.class);

            mbean.getMBeanInfo();
        }
        catch (AssertionError ignored) {
            return;
        }

        assert false;
    }

    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    public void testEmptyNameMBeanInfo() throws Exception {
        try {
            StandardMBean mbean = new IgniteStandardMXBean(new GridMBeanImplementation(),
                GridMBeanInterfaceEmptyName.class);

            mbean.getMBeanInfo();
        }
        catch (AssertionError ignored) {
            return;
        }

        assert false;
    }

    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    public void testIgniteKernalReturnsValidMBeanInfo() throws Exception {
        try {
            IgniteMXBean ignite = (IgniteMXBean)startGrid();

            assertNotNull(ignite.getUserAttributesFormatted());
            assertNotNull(ignite.getLifecycleBeansFormatted());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Super interface for {@link GridMBeanInterface}.
     */
    public static interface GridMBeanSuperInterface {
        /**
         * Test getter.
         *
         * @return Some string.
         */
        @MXBeanDescription("MBeanROGetter.")
        public String getROData();

        /**
         * Test MBean operation.
         *
         * @param someData Some data.
         * @return Some string.
         */
        @MXBeanDescription("MBeanSuperOperation.")
        @MXBeanParametersNames({"someData"})
        @MXBeanParametersDescriptions({"MBeanOperationParameter1."})
        public String doSomethingSuper(String someData);
    }

    /**
     * Test MBean interface.
     */
    @MXBeanDescription("MBeanDescription.")
    public static interface GridMBeanInterface extends GridMBeanSuperInterface {
        /**
         * Test getter.
         *
         * @return Some string.
         */
        @MXBeanDescription("MBeanWritableGetter.")
        public String getWritableData();

        /**
         * Test setter.
         *
         * @param data Some string.
         */
        public void setWritableData(String data);

        /**
         * Test getter.
         *
         * @return Some string.
         */
        @MXBeanDescription("MBeanReadonlyGetter.")
        public String getReadOnlyData();

        /**
         * Test boolean getter.
         *
         * @return Some string.
         */
        @MXBeanDescription("MBeanWritableIsGetter.")
        public boolean isWritable();

        /**
         * Test boolean setter.
         *
         * @param isWritable Just a boolean.
         */
        public void setWritable(boolean isWritable);

        /**
         * Test MBean operation.
         *
         * @param ignored Some value.
         * @param someData Some data.
         * @return Some string.
         */
        @MXBeanDescription("MBeanOperation.")
        @MXBeanParametersNames({"ignored", "someData"})
        @MXBeanParametersDescriptions({"MBeanOperationParameter1.", "MBeanOperationParameter2."})
        public String doSomething(boolean ignored, String someData);
    }

    /**
     * Test MBean interface.
     */
    public static interface GridMBeanInterfaceBad {
        /**
         * Test MBean operation.
         *
         * @param ignored Some value.
         * @param someData Some data.
         * @return Some string.
         */
        @MXBeanDescription("MBeanOperation.")
        @MXBeanParametersNames({"ignored"})
        @MXBeanParametersDescriptions({"MBeanOperationParameter1.", "MBeanOperationParameter2."})
        public String doSomethingBad(boolean ignored, String someData);
    }

    /**
     * Test MBean interface.
     */
    public static interface GridMBeanInterfaceEmptyDescription {
        /**
         * Test MBean operation.
         *
         * @param ignored Some value.
         * @param someData Some data.
         * @return Some string.
         */
        @MXBeanDescription("")
        public String doSomethingBad(boolean ignored, String someData);
    }

    /**
     * Test MBean interface.
     */
    public static interface GridMBeanInterfaceEmptyName {
        /**
         * Test MBean operation.
         *
         * @param ignored Some value.
         * @param someData Some data.
         * @return Some string.
         */
        @MXBeanParametersNames({"", "someData"})
        public String doSomethingBadAgain(boolean ignored, String someData);
    }

    /**
     * Test MBean interface.
     */
    public static interface GridMBeanInterfaceBadAgain {
        /**
         * Test MBean operation.
         *
         * @param ignored Some value.
         * @param someData Some data.
         * @return Some string.
         */
        @MXBeanDescription("MBeanOperation.")
        @MXBeanParametersNames({"ignored", "someData"})
        @MXBeanParametersDescriptions({"MBeanOperationParameter1."})
        public String doSomethingBadAgain(boolean ignored, String someData);
    }

    /**
     * Test MBean implementation.
     */
    public class GridMBeanImplementation implements GridMBeanInterface, GridMBeanInterfaceBad,
        GridMBeanInterfaceBadAgain, GridMBeanInterfaceEmptyDescription, GridMBeanInterfaceEmptyName {

        /** {@inheritDoc} */
        @Override public String doSomething(boolean ignored, String someData) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String getReadOnlyData() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String getWritableData() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void setWritableData(String data) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public String doSomethingBad(boolean ignored, String someData) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String doSomethingBadAgain(boolean ignored, String someData) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isWritable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void setWritable(boolean isWritable) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public String getROData() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String doSomethingSuper(String someData) {
            return null;
        }
    }
}
