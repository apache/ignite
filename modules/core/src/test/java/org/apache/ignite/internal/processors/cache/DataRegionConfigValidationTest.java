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
package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class DataRegionConfigValidationTest extends GridCommonAbstractTest {
    /** */
    private static final String VALID_DEFAULT_MEM_PLC_NAME = "valid_dlft_mem_plc";

    /** */
    private static final String VALID_USER_MEM_PLC_NAME = "valid_user_mem_plc";

    /** */
    private static final String MISSING_DEFAULT_MEM_PLC_NAME = "missing_mem_plc";

    /** Configuration violation type to check. */
    private ValidationViolationType violationType;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        DataRegionConfiguration[] plcs = null;

        switch (violationType) {
            case NAMES_CONFLICT:
                plcs = createPlcsWithNamesConflictCfg();

                break;

            case SYSTEM_DATA_REGION_NAME_MISUSE:
                plcs = createRegionWithReservedNameMisuseCfg();

                break;

            case TOO_SMALL_MEMORY_SIZE:
                plcs = createTooSmallMemoryCfg();

                break;

            case NULL_NAME_ON_USER_DEFINED_POLICY:
                plcs = createPlcWithNullName();

                break;

            case MISSING_USER_DEFINED_DEFAULT:
                plcs = createMissingUserDefinedDefault();

                memCfg.setDefaultDataRegionName(MISSING_DEFAULT_MEM_PLC_NAME);

                break;

            case TOO_SMALL_USER_DEFINED_DFLT_MEM_PLC_SIZE:
                memCfg.setDefaultDataRegionSize(1);

                break;

            case DEFAULT_SIZE_IS_DEFINED_TWICE:
                plcs = createValidUserDefault();

                memCfg.setDefaultDataRegionName(VALID_DEFAULT_MEM_PLC_NAME);
                memCfg.setDefaultDataRegionSize(10 * 1014 * 1024);

                break;

            case MAX_SIZE_IS_SMALLER_THAN_INITIAL_SIZE:
                plcs = createMaxSizeSmallerThanInitialSize();

                break;

            case LTE_ZERO_RATE_TIME_INTERVAL:
                plcs = createRateTimeIntervalIsNegative();

                break;

            case LTE_ZERO_SUB_INTERVALS:
                plcs = createSubIntervalsIsNegative();

                break;

            default:
                fail("Violation type was not configured: " + violationType);
        }

        memCfg.setDataRegionConfigurations(plcs);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /**
     *
     */
    private DataRegionConfiguration[] createSubIntervalsIsNegative() {
        DataRegionConfiguration[] res = new DataRegionConfiguration[1];

        res[0] = createDataRegion(VALID_DEFAULT_MEM_PLC_NAME, 100 * 1024 * 1024, 100 * 1024 * 1024);
        res[0].setSubIntervals(-10);

        return res;
    }

    /**
     *
     */
    private DataRegionConfiguration[] createRateTimeIntervalIsNegative() {
        DataRegionConfiguration[] res = new DataRegionConfiguration[1];

        res[0] = createDataRegion(VALID_DEFAULT_MEM_PLC_NAME, 100 * 1024 * 1024, 100 * 1024 * 1024);
        res[0].setRateTimeInterval(-10);

        return res;
    }

    /**
     *
     */
    private DataRegionConfiguration[] createValidUserDefault() {
        DataRegionConfiguration[] res = new DataRegionConfiguration[1];

        res[0] = createDataRegion(VALID_DEFAULT_MEM_PLC_NAME, 100 * 1024 * 1024, 100 * 1024 * 1024);

        return res;
    }

    /**
     *
     */
    private DataRegionConfiguration[] createMissingUserDefinedDefault() {
        DataRegionConfiguration[] res = new DataRegionConfiguration[1];

        res[0] = createDataRegion(VALID_USER_MEM_PLC_NAME, 10 * 1024 * 1024, 10 * 1024 * 1024);

        return res;
    }

    /**
     *
     */
    private DataRegionConfiguration[] createPlcWithNullName() {
        DataRegionConfiguration[] res = new DataRegionConfiguration[1];

        res[0] = createDataRegion(null, 10 * 1024 * 1024, 10 * 1024 * 1024);

        return res;
    }

    /**
     *
     */
    private DataRegionConfiguration[] createTooSmallMemoryCfg() {
        DataRegionConfiguration[] res = new DataRegionConfiguration[1];

        res[0] = createDataRegion(VALID_DEFAULT_MEM_PLC_NAME, 10, 10);

        return res;
    }

    /**
     *
     */
    private DataRegionConfiguration[] createRegionWithReservedNameMisuseCfg() {
        DataRegionConfiguration[] res = new DataRegionConfiguration[1];

        res[0] = createDataRegion("sysDataReg", 1024 * 1024, 1024 * 1024);

        return res;
    }

    /**
     *
     */
    private DataRegionConfiguration[] createPlcsWithNamesConflictCfg() {
        DataRegionConfiguration[] res = new DataRegionConfiguration[2];

        res[0] = createDataRegion("cflt0", 10 * 1024 * 1024, 10 * 1024 * 1024);
        res[1] = createDataRegion("cflt0", 10 * 1024 * 1024, 10 * 1024 * 1024);

        return res;
    }

    /**
     *
     */
    private DataRegionConfiguration[] createMaxSizeSmallerThanInitialSize() {
        DataRegionConfiguration[] res = new DataRegionConfiguration[1];

        res[0] = createDataRegion("invalidSize", 100 * 1024 * 1024, 10 * 1024 * 1024);

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @param name Name of DataRegionConfiguration.
     * @param initialSize Initial size of DataRegionConfiguration in bytes.
     * @param maxSize Max size of DataRegionConfiguration in bytes.
     */
    private DataRegionConfiguration createDataRegion(String name, long initialSize, long maxSize) {
        DataRegionConfiguration plc = new DataRegionConfiguration();

        plc.setName(name);
        plc.setInitialSize(initialSize);
        plc.setMaxSize(maxSize);

        return plc;
    }

    /**
     * 'sysDataReg' name is reserved for DataRegionConfiguration for system caches.
     */
    public void testReservedDataRegionMisuse() throws Exception {
        violationType = ValidationViolationType.SYSTEM_DATA_REGION_NAME_MISUSE;

        doTest(violationType);
    }

    /**
     * If user defines default is must be presented among configured memory policies.
     */
    public void testMissingUserDefinedDefault() throws Exception {
        violationType = ValidationViolationType.MISSING_USER_DEFINED_DEFAULT;

        doTest(violationType);
    }

    /**
     * Names of all MemoryPolicies must be distinct.
     */
    public void testNamesConflict() throws Exception {
        violationType = ValidationViolationType.NAMES_CONFLICT;

        doTest(violationType);
    }

    /**
     * User-defined policy must have a non-null non-empty name.
     */
    public void testNullNameOnUserDefinedPolicy() throws Exception {
        violationType = ValidationViolationType.NULL_NAME_ON_USER_DEFINED_POLICY;

        doTest(violationType);
    }

    /**
     * DataRegion must be configured with size of at least 1MB.
     */
    public void testMemoryTooSmall() throws Exception {
        violationType = ValidationViolationType.TOO_SMALL_MEMORY_SIZE;

        doTest(violationType);
    }

    /**
     * DataRegion must be configured with size of at least 1MB.
     */
    public void testMaxSizeSmallerThanInitialSize() throws Exception {
        violationType = ValidationViolationType.MAX_SIZE_IS_SMALLER_THAN_INITIAL_SIZE;

        doTest(violationType);
    }

    /**
     * User-defined size of default DataRegion must be at least 1MB.
     */
    public void testUserDefinedDefaultMemoryTooSmall() throws Exception {
        violationType = ValidationViolationType.TOO_SMALL_USER_DEFINED_DFLT_MEM_PLC_SIZE;

        doTest(violationType);
    }

    /**
     * Defining size of default DataRegion twice with and through <b>defaultMemoryPolicySize</b> property
     * and using <b>DataRegionConfiguration</b> description is prohibited.
     */
    public void testDefaultMemoryPolicySizeDefinedTwice() throws Exception {
        violationType = ValidationViolationType.DEFAULT_SIZE_IS_DEFINED_TWICE;

        doTest(violationType);
    }

    /**
     *
     */
    public void testRateTimeIntervalPropertyIsNegative() throws Exception {
        violationType = ValidationViolationType.LTE_ZERO_RATE_TIME_INTERVAL;

        doTest(violationType);
    }

    /**
     *
     */
    public void testSubIntervalsPropertyIsNegative() throws Exception {
        violationType = ValidationViolationType.LTE_ZERO_SUB_INTERVALS;

        doTest(violationType);
    }

    /**
     * Tries to start ignite node with invalid configuration and checks that corresponding exception is thrown.
     *
     * @param violationType Configuration violation type.
     */
    private void doTest(ValidationViolationType violationType) throws Exception {
        try {
            startGrid(0);
        }
        catch (IgniteCheckedException e) {
            Throwable c = e.getCause();

            assertTrue(c != null);
            assertTrue(c.getMessage().contains(violationType.violationMsg));

            return;
        }

        fail("Expected exception hasn't been thrown");
    }

    /**
     *
     */
    private enum ValidationViolationType {
        /** */
        NAMES_CONFLICT("Two MemoryPolicies have the same name: "),

        /** */
        SYSTEM_DATA_REGION_NAME_MISUSE("'sysDataReg' policy name is reserved for internal use."),

        /** */
        TOO_SMALL_MEMORY_SIZE("DataRegion must have size more than 10MB "),

        /** */
        NULL_NAME_ON_USER_DEFINED_POLICY("User-defined DataRegionConfiguration must have non-null and non-empty name."),

        /** */
        MISSING_USER_DEFINED_DEFAULT("User-defined default DataRegion name must be presented among configured MemoryPolices: "),

        /** */
        DEFAULT_SIZE_IS_DEFINED_TWICE("User-defined DataRegion configuration and defaultMemoryPolicySize properties are set at the same time."),

        /** */
        TOO_SMALL_USER_DEFINED_DFLT_MEM_PLC_SIZE("User-defined default DataRegion size is less than 1MB."),

        /** */
        MAX_SIZE_IS_SMALLER_THAN_INITIAL_SIZE("DataRegion maxSize must not be smaller than initialSize"),

        /** Case when rateTimeInterval property of DataRegionConfiguration is less than or equals zero. */
        LTE_ZERO_RATE_TIME_INTERVAL("Rate time interval must be greater than zero " +
            "(use DataRegionConfiguration.rateTimeInterval property to adjust the interval)"),

        /** Case when subIntervals property of DataRegionConfiguration is less than or equals zero. */
        LTE_ZERO_SUB_INTERVALS("Sub intervals must be greater than zero " +
            "(use DataRegionConfiguration.subIntervals property to adjust the sub intervals)");

        /**
         * @param violationMsg Violation message.
         */
        ValidationViolationType(String violationMsg) {
            this.violationMsg = violationMsg;
        }

        /** */
        String violationMsg;
    }
}
