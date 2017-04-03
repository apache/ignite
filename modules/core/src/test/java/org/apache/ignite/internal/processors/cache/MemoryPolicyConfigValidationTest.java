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

            case RESERVED_MEMORY_POLICY_MISUSE:
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

                memCfg.setDefaultMemoryPolicyName("missingMemoryPolicyName");

                break;
        }

        memCfg.setMemoryPolicies(plcs);

        cfg.setMemoryConfiguration(memCfg);

        return cfg;
    }

    private MemoryPolicyConfiguration[] createMissingUserDefinedDefault() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[1];

        res[0] = createMemoryPolicy("presentedPolicyCfg", 10 * 1024 * 1024);

        return res;
    }

    private MemoryPolicyConfiguration[] createPlcWithNullName() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[1];

        res[0] = createMemoryPolicy(null, 10 * 1024 * 1024);

        return res;
    }

    /**
     *
     */
    private MemoryPolicyConfiguration[] createTooSmallMemoryCfg() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[1];

        res[0] = createMemoryPolicy("dflt", 10);

        return res;
    }

    /**
     *
     */
    private MemoryPolicyConfiguration[] createPlcWithReservedNameMisuseCfg() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[1];

        res[0] = createMemoryPolicy("sysMemPlc", 1024 * 1024);

        return res;
    }

    /**
     *
     */
    private MemoryPolicyConfiguration[] createPlcsWithNamesConflictCfg() {
        MemoryPolicyConfiguration[] res = new MemoryPolicyConfiguration[2];

        res[0] = createMemoryPolicy("cflt0", 1024 * 1024);
        res[1] = createMemoryPolicy("cflt0", 1024 * 1024);

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @param name Name of MemoryPolicyConfiguration.
     * @param size Size of MemoryPolicyConfiguration in bytes.
     */
    private MemoryPolicyConfiguration createMemoryPolicy(String name, long size) {
        MemoryPolicyConfiguration plc = new MemoryPolicyConfiguration();

        plc.setName(name);
        plc.setSize(size);

        return plc;
    }

    /**
     * 'sysMemPlc' name is reserved for MemoryPolicyConfiguration for system caches.
     */
    public void testReservedMemoryPolicyMisuse() throws Exception {
        violationType = ValidationViolationType.RESERVED_MEMORY_POLICY_MISUSE;

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
        RESERVED_MEMORY_POLICY_MISUSE("'sysMemPlc' policy name is reserved for internal use."),

        /** */
        TOO_SMALL_MEMORY_SIZE("MemoryPolicy must have size more than 1MB: "),

        /** */
        NULL_NAME_ON_USER_DEFINED_POLICY("User-defined MemoryPolicyConfiguration must have non-null and non-empty name."),

        /** */
        MISSING_USER_DEFINED_DEFAULT("User-defined default MemoryPolicy name must be presented among configured MemoryPolices: ");

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
