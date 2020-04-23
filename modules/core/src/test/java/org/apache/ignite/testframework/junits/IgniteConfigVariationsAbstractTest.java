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

package org.apache.ignite.testframework.junits;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.configvariations.ConfigVariations;
import org.apache.ignite.testframework.configvariations.ConfigVariationsFactory;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.configvariations.VariationsTestsConfig;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;

/**
 * Common abstract test for Ignite tests based on configurations variations.
 */
public abstract class IgniteConfigVariationsAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static final int SERVER_NODE_IDX = 0;

    /** */
    protected static final int CLIENT_NODE_IDX = 1;

    /** */
    protected static int testedNodeIdx;

    /** */
    private static final File workDir = new File(U.getIgniteHome() + File.separator + "workOfConfigVariationsTests");

    /**
     * Dummy initial stub to just let people launch test classes not from suite.
     * Real configurations are assigned dynamically through reflection in
     * {@link ConfigVariationsTestSuiteBuilder#makeTestClass(String, VariationsTestsConfig)} method.
     */
    protected VariationsTestsConfig testsCfg = dummyCfg();

    /** */
    protected volatile DataMode dataMode = DataMode.PLANE_OBJECT;

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName(int idx) {
        return getTestIgniteInstanceName() + idx;
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return "testGrid";
    }

    /** {@inheritDoc} */
    @Override protected boolean isSafeTopology() {
        return false;
    }

    /** Check that test name is not null. */
    @Before
    public void checkTestName() {
        assert getName() != null : "getName returned null";
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        assert testsCfg != null;

        if (Ignition.allGrids().size() != testsCfg.gridCount()) {
            info("All nodes will be stopped, new " + testsCfg.gridCount() + " nodes will be started.");

            Ignition.stopAll(true);

            FileUtils.deleteDirectory(workDir);

            info("Ignite's 'work' directory has been cleaned.");

            startGrids(testsCfg.gridCount());

            for (int i = 0; i < testsCfg.gridCount(); i++)
                info("Grid " + i + ": " + grid(i).localNode().id());
        }

        assert testsCfg.testedNodeIndex() >= 0 : "testedNodeIdx: " + testedNodeIdx;

        testedNodeIdx = testsCfg.testedNodeIndex();

        if (testsCfg.withClients()) {
            for (int i = 0; i < gridCount(); i++)
                assertEquals("i: " + i, expectedClient(getTestIgniteInstanceName(i)),
                    (boolean)grid(i).configuration().isClientMode());
        }
    }

    /**
     * @param testGridName Name.
     * @return {@code True} if node is client should be client.
     */
    protected boolean expectedClient(String testGridName) {
        return getTestIgniteInstanceName(CLIENT_NODE_IDX).equals(testGridName);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        try {
            if (testsCfg.isStopNodes()) {
                info("Stopping all grids...");

                stopAllGrids();

                FileUtils.deleteDirectory(workDir);

                info("Ignite's 'work' directory has been cleaned.");

                memoryUsage();

                System.gc();

                memoryUsage();
            }
        }
        finally {
            unconditionalCleanupAfterTests();
        }
    }

    /** */
    protected void unconditionalCleanupAfterTests() {
        testedNodeIdx = 0;

        testsCfg = dummyCfg();
    }

    /**
     * Prints memory usage.
     */
    private void memoryUsage() {
        int mb = 1024 * 1024;

        Runtime runtime = Runtime.getRuntime();

        info("##### Heap utilization statistics [MB] #####");
        info("Used Memory  (mb): " + (runtime.totalMemory() - runtime.freeMemory()) / mb);
        info("Free Memory  (mb): " + runtime.freeMemory() / mb);
        info("Total Memory (mb): " + runtime.totalMemory() / mb);
        info("Max Memory   (mb): " + runtime.maxMemory() / mb);
    }

    /** {@inheritDoc} */
    @Override protected String testClassDescription() {
        assert testsCfg != null: "Tests should be run using test suite.";

        return super.testClassDescription() + '-' + testsCfg.description() + '-' + testsCfg.gridCount() + "-node(s)";
    }

    /** {@inheritDoc} */
    @Override protected String testDescription() {
        assert testsCfg != null: "Tests should be run using test suite.";

        return super.testDescription() + '-' + testsCfg.description() + '-' + testsCfg.gridCount() + "-node(s)";
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        IgniteConfiguration resCfg = testsCfg.configurationFactory().getConfiguration(igniteInstanceName, cfg);

        resCfg.setWorkDirectory(workDir.getAbsolutePath());

        if (testsCfg.withClients())
            resCfg.setClientMode(expectedClient(igniteInstanceName));

        resCfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(200L * 1024 * 1024)
                ));

        return resCfg;
    }

    /** */
    protected final int gridCount() {
        return testsCfg.gridCount();
    }

    /**
     * @return Count of clients.
     */
    protected int clientsCount() {
        int cnt = 0;

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).configuration().isClientMode())
                cnt++;
        }

        return cnt;
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx grid() {
        throw new UnsupportedOperationException("Not supported, grid(int idx) or testedGrid() should be used instead.");
    }

    /**
     * @return Grid which should be tested.
     */
    protected IgniteEx testedGrid() {
        return grid(testedNodeIdx);
    }

    /**
     * @return Tested grid in client mode or not.
     */
    protected boolean isClientMode() {
        return grid(testedNodeIdx).configuration().isClientMode();
    }

    /**
     * @return Count of server nodes at topology.
     */
    protected int serversGridCount() {
        int cnt = 0;

        for (int i = 0; i < gridCount(); i++) {
            if (!grid(i).configuration().isClientMode())
                cnt++;
        }

        return cnt;
    }

    /**
     * Runs in all data modes.
     *
     * @throws Exception If failed.
     */
    protected void runInAllDataModes(TestRunnable call, DataMode... dataModes) throws Exception {
        if (F.isEmpty(dataModes))
            dataModes = DataMode.values();

        for (int i = 0; i < dataModes.length; i++) {
            dataMode = dataModes[i];

            if (!isCompatible()) {
                info("Skipping test in data mode: " + dataMode);

                continue;
            }

            info("Running test in data mode: " + dataMode);

            if (i != 0)
                beforeTest();

            try {
                call.run();
            }
            catch (Throwable e) {
                e.printStackTrace();

                throw e;
            }
            finally {
                if (i + 1 != DataMode.values().length)
                    afterTest();
            }
        }
    }

    /**
     * @param keyId Key Id.
     * @return Key.
     * @see #valueOf(Object)
     */
    public Object key(int keyId) {
        return key(keyId, dataMode);
    }

    /**
     * @param valId Key Id.
     * @return Value.
     * @see #valueOf(Object)
     */
    public Object value(int valId) {
        return value(valId, dataMode);
    }

    /**
     * @param keyId Key Id.
     * @param mode Mode.
     * @return Key.
     */
    public static Object key(int keyId, DataMode mode) {
        if (mode == null)
            mode = DataMode.SERIALIZABLE;

        switch (mode) {
            case SERIALIZABLE:
                return new SerializableObject(keyId);
            case CUSTOM_SERIALIZABLE:
                return new CustomSerializableObject(keyId);
            case EXTERNALIZABLE:
                return new ExternalizableObject(keyId);
            case PLANE_OBJECT:
                return new PlaneObject(keyId);
            case BINARILIZABLE:
                return new BinarylizableObject(keyId);
            default:
                throw new IllegalArgumentException("mode: " + mode);
        }
    }

    /**
     * @param obj Key or value object
     * @return Value.
     */
    public static int valueOf(Object obj) {
        assertNotNull(obj);

        if (obj instanceof TestObject)
            return ((TestObject)obj).value();
        else
            throw new IllegalArgumentException("Unknown tested object type: " + obj);
    }

    /**
     * @param idx Index.
     * @param mode Mode.
     * @return Value.
     */
    public static Object value(int idx, DataMode mode) {
        if (mode == null)
            mode = DataMode.SERIALIZABLE;

        switch (mode) {
            case SERIALIZABLE:
                return new SerializableObject(idx);
            case CUSTOM_SERIALIZABLE:
                return new CustomSerializableObject(idx);
            case EXTERNALIZABLE:
                return new ExternalizableObject(idx);
            case PLANE_OBJECT:
                return new PlaneObject(idx);
            case BINARILIZABLE:
                return new BinarylizableObject(idx);
            default:
                throw new IllegalArgumentException("mode: " + mode);
        }
    }

    /** */
    private VariationsTestsConfig dummyCfg() {
        return new VariationsTestsConfig(
            new ConfigVariationsFactory(null, new int[] {0}, ConfigVariations.cacheBasicSet(), new int[] {0}),
            "Dummy config", false, null, 1, false);
    }

    /**
     *
     */
    public static interface TestObject {
        /**
         * @return Value.
         */
        public int value();
    }

    /**
     *
     */
    public static class PlaneObject implements TestObject {
        /** */
        protected int val;

        /** */
        protected String strVal;

        /** */
        protected TestEnum enumVal;

        /**
         * Default constructor must be accessible for deserialize subclasses by JDK serialization API.
         */
        PlaneObject() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        PlaneObject(int val) {
            this.val = val;
            strVal = "val" + val;

            TestEnum[] values = TestEnum.values();
            enumVal = values[Math.abs(val) % values.length];
        }

        /** {@inheritDoc} */
        @Override public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof PlaneObject))
                return false;

            PlaneObject val = (PlaneObject)o;

            return getClass().equals(o.getClass()) && this.val == val.val && enumVal == val.enumVal
                && strVal.equals(val.strVal);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return getClass().getSimpleName() + "[" +
                "val=" + val +
                ", strVal='" + strVal + '\'' +
                ", enumVal=" + enumVal +
                ']';
        }
    }

    /**
     *
     */
    protected static class SerializableObject implements Serializable, TestObject {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        protected int val;

        /** */
        protected String strVal;

        /** */
        protected TestEnum enumVal;

        /**
         * Default constructor.
         */
        public SerializableObject() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        public SerializableObject(int val) {
            this.val = val;
            strVal = "val" + val;

            TestEnum[] values = TestEnum.values();
            enumVal = values[Math.abs(val) % values.length];
        }

        /** {@inheritDoc} */
        @Override public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof SerializableObject))
                return false;

            SerializableObject val = (SerializableObject)o;

            return getClass().equals(o.getClass()) && this.val == val.val && enumVal == val.enumVal
                && strVal.equals(val.strVal);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return getClass().getSimpleName() + "[" +
                "val=" + val +
                ", strVal='" + strVal + '\'' +
                ", enumVal=" + enumVal +
                ']';
        }
    }

    /**
     *
     */
    protected static class CustomSerializableObject extends PlaneObject implements Serializable {
        /** */
        private static final long serialVersionUID = 0;

        /**
         * Default constructor.
         */
        public CustomSerializableObject() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        public CustomSerializableObject(int val) {
            super(val);
        }

        /**
         * Custom serialization of superclass because {@link PlaneObject} is non-serializable.
         *
         * @param out output stream.
         * @throws IOException if de-serialization failed.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeInt(val);
            out.writeObject(strVal);
            out.writeObject(enumVal);
        }

        /**
         * Custom deserialization of superclass because {@link PlaneObject} is non-serializable.
         *
         * @param in input stream
         * @throws IOException if de-serialization failed.
         * @throws ClassNotFoundException if de-serialization failed.
         */
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            val = in.readInt();
            strVal = (String)in.readObject();
            enumVal = (TestEnum)in.readObject();
        }
    }

    /**
     *
     */
    private static class ExternalizableObject extends PlaneObject implements Externalizable {
        /** */
        private static final long serialVersionUID = 0;

        /**
         * Default constructor.
         */
        public ExternalizableObject() {
            super(-1);
        }

        /**
         * @param val Value.
         */
        ExternalizableObject(int val) {
            super(val);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(val);
            out.writeObject(strVal);
            out.writeObject(enumVal);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            val = in.readInt();
            strVal = (String)in.readObject();
            enumVal = (TestEnum)in.readObject();
        }
    }

    /**
     *
     */
    public static class BinarylizableObject extends PlaneObject implements Binarylizable {
        /**
         * Default constructor.
         */
        public BinarylizableObject() {
            super(-1);
        }

        /**
         * @param val Value.
         */
        public BinarylizableObject(int val) {
            super(val);
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("val", val);
            writer.writeString("strVal", strVal);
            writer.writeEnum("enumVal", enumVal);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            val = reader.readInt("val");
            strVal = reader.readString("strVal");
            enumVal = reader.readEnum("enumVal");
        }
    }

    /**
     * Data mode.
     */
    public enum DataMode {
        /** Serializable objects. */
        SERIALIZABLE,

        /** Serializable objects with custom serialization. */
        CUSTOM_SERIALIZABLE,

        /** Externalizable objects. */
        EXTERNALIZABLE,

        /** Objects without Serializable and Externalizable. */
        PLANE_OBJECT,

        /** Binarylizable objects. Compatible only with binary marshaller */
        BINARILIZABLE
    }

    /**
     *
     */
    private enum TestEnum {
        /** */
        TEST_VALUE_1,

        /** */
        TEST_VALUE_2,

        /** */
        TEST_VALUE_3
    }

    /**
     *
     */
    public interface TestRunnable {
        /**
         * @throws Exception If failed.
         */
        void run() throws Exception;
    }

    /**
     * Check test compatibility with current data mode
     *
     * @return {@code True} if compatible.
     * @throws Exception If failed.
     */
    protected boolean isCompatible() throws Exception {
        switch (dataMode) {
            case BINARILIZABLE:
            case PLANE_OBJECT:
                return !(getConfiguration().getMarshaller() instanceof JdkMarshaller);
        }
        return false;
    }
}
