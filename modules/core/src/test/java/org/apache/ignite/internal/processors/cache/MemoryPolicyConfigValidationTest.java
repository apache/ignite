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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class MemoryPolicyConfigValidationTest extends GridCommonAbstractTest {
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

        MemoryConfiguration memCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration[] plcs = null;

        switch (violationType) {
            case NAMES_CONFLICT:
                plcs = createPlcsWithNamesConflictCfg();

                break;

            case SYSTEM_MEMORY_POLICY_NAME_MISUSE:
                plcs = createPlcWithReservedNameMisuseCfg();

                break;

            case TOO_SMALL_MEMORY_SIZE:
                plcs = createTooSmallMemoryCfg();

                break;

            case NULL_NAME_ON_USER_DEFINED_POLICY:
                plcs = createPlcWithNullName();

                break;

            case MISSING_USER_DEFINED_DEFAULT:
                plcs = createMissingUserDefinedDefault();

                memCfg.setDefaultMemoryPolicyName(MISSING_DEFAULT_MEM_PLC_NAME);

                break;

            case TOO_SMALL_USER_DEFINED_DFLT_MEM_PLC_SIZE:
                memCfg.setDefaultMemoryPolicySize(1);

                break;

            case DEFAULT_SIZE_IS_DEFINED_TWICE:
                plcs = createValidUserDefault();

                memCfg.setDefaultMemoryPolicyName(VALID_DEFAULT_MEM_PLC_NAME);
                memCfg.setDefaultMemoryPolicySize(10 * 1014 * 1024);

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

        memCfg.setMemoryPolicies(plcs);

        cfg.setMemoryConfiguration(memCfg);

        return cfg;
    }

    /**
     *
     */
    private MemoryPolicyConfiguration[] createSubIntervalsIsNegative() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[1];

        res[0] = createMemoryPolicy(VALID_DEFAULT_MEM_PLC_NAME, 100 * 1024 * 1024, 100 * 1024 * 1024);
        res[0].setSubIntervals(-10);

        return res;
    }

    /**
     *
     */
    private MemoryPolicyConfiguration[] createRateTimeIntervalIsNegative() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[1];

        res[0] = createMemoryPolicy(VALID_DEFAULT_MEM_PLC_NAME, 100 * 1024 * 1024, 100 * 1024 * 1024);
        res[0].setRateTimeInterval(-10);

        return res;
    }

    /**
     *
     */
    private MemoryPolicyConfiguration[] createValidUserDefault() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[1];

        res[0] = createMemoryPolicy(VALID_DEFAULT_MEM_PLC_NAME, 100 * 1024 * 1024, 100 * 1024 * 1024);

        return res;
    }

    /**
     *
     */
    private MemoryPolicyConfiguration[] createMissingUserDefinedDefault() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[1];

        res[0] = createMemoryPolicy(VALID_USER_MEM_PLC_NAME, 10 * 1024 * 1024, 10 * 1024 * 1024);

        return res;
    }

    /**
     *
     */
    private MemoryPolicyConfiguration[] createPlcWithNullName() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[1];

        res[0] = createMemoryPolicy(null, 10 * 1024 * 1024, 10 * 1024 * 1024);

        return res;
    }

    /**
     *
     */
    private MemoryPolicyConfiguration[] createTooSmallMemoryCfg() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[1];

        res[0] = createMemoryPolicy(VALID_DEFAULT_MEM_PLC_NAME, 10, 10);

        return res;
    }

    /**
     *
     */
    private MemoryPolicyConfiguration[] createPlcWithReservedNameMisuseCfg() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[1];

        res[0] = createMemoryPolicy("sysMemPlc", 1024 * 1024, 1024 * 1024);

        return res;
    }

    /**
     *
     */
    private MemoryPolicyConfiguration[] createPlcsWithNamesConflictCfg() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[2];

        res[0] = createMemoryPolicy("cflt0", 10 * 1024 * 1024, 10 * 1024 * 1024);
        res[1] = createMemoryPolicy("cflt0", 10 * 1024 * 1024, 10 * 1024 * 1024);

        return res;
    }

    /**
     *
     */
    private MemoryPolicyConfiguration[] createMaxSizeSmallerThanInitialSize() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[1];

        res[0] = createMemoryPolicy("invalidSize", 100 * 1024 * 1024, 10 * 1024 * 1024);

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @param name Name of MemoryPolicyConfiguration.
     * @param initialSize Initial size of MemoryPolicyConfiguration in bytes.
     * @param maxSize Max size of MemoryPolicyConfiguration in bytes.
     */
    private MemoryPolicyConfiguration createMemoryPolicy(String name, long initialSize, long maxSize) {
        MemoryPolicyConfiguration plc = new MemoryPolicyConfiguration();

        plc.setName(name);
        plc.setInitialSize(initialSize);
        plc.setMaxSize(maxSize);

        return plc;
    }

    /**
     * 'sysMemPlc' name is reserved for MemoryPolicyConfiguration for system caches.
     */
    public void testReservedMemoryPolicyMisuse() throws Exception {
        violationType = ValidationViolationType.SYSTEM_MEMORY_POLICY_NAME_MISUSE;

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
     * MemoryPolicy must be configured with size of at least 1MB.
     */
    public void testMemoryTooSmall() throws Exception {
        violationType = ValidationViolationType.TOO_SMALL_MEMORY_SIZE;

        doTest(violationType);
    }

    /**
     * MemoryPolicy must be configured with size of at least 1MB.
     */
    public void testMaxSizeSmallerThanInitialSize() throws Exception {
        violationType = ValidationViolationType.MAX_SIZE_IS_SMALLER_THAN_INITIAL_SIZE;

        doTest(violationType);
    }

    /**
     * User-defined size of default MemoryPolicy must be at least 1MB.
     */
    public void testUserDefinedDefaultMemoryTooSmall() throws Exception {
        violationType = ValidationViolationType.TOO_SMALL_USER_DEFINED_DFLT_MEM_PLC_SIZE;

        doTest(violationType);
    }

    /**
     * Defining size of default MemoryPolicy twice with and through <b>defaultMemoryPolicySize</b> property
     * and using <b>MemoryPolicyConfiguration</b> description is prohibited.
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
        SYSTEM_MEMORY_POLICY_NAME_MISUSE("'sysMemPlc' policy name is reserved for internal use."),

        /** */
        TOO_SMALL_MEMORY_SIZE("MemoryPolicy must have size more than 10MB "),

        /** */
        NULL_NAME_ON_USER_DEFINED_POLICY("User-defined MemoryPolicyConfiguration must have non-null and non-empty name."),

        /** */
        MISSING_USER_DEFINED_DEFAULT("User-defined default MemoryPolicy name must be presented among configured MemoryPolices: "),

        /** */
        DEFAULT_SIZE_IS_DEFINED_TWICE("User-defined MemoryPolicy configuration and defaultMemoryPolicySize properties are set at the same time."),

        /** */
        TOO_SMALL_USER_DEFINED_DFLT_MEM_PLC_SIZE("User-defined default MemoryPolicy size is less than 1MB."),

        /** */
        MAX_SIZE_IS_SMALLER_THAN_INITIAL_SIZE("MemoryPolicy maxSize must not be smaller than initialSize"),

        /** Case when rateTimeInterval property of MemoryPolicyConfiguration is less than or equals zero. */
        LTE_ZERO_RATE_TIME_INTERVAL("Rate time interval must be greater than zero " +
            "(use MemoryPolicyConfiguration.rateTimeInterval property to adjust the interval)"),

        /** Case when subIntervals property of MemoryPolicyConfiguration is less than or equals zero. */
        LTE_ZERO_SUB_INTERVALS("Sub intervals must be greater than zero " +
            "(use MemoryPolicyConfiguration.subIntervals property to adjust the sub intervals)");

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
