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

package org.apache.ignite.internal;

import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import javax.management.ObjectName;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MBEAN_APPEND_CLASS_LOADER_ID;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MBEAN_APPEND_JVM_ID;
import static org.apache.ignite.internal.util.IgniteUtils.JMX_DOMAIN;

/**
 * Tests for the standard JMX beans registered by the kernal.
 */
public class GridMBeansTest extends GridCommonAbstractTest {
    /** Executor name for setExecutorConfiguration. */
    private static final String CUSTOM_EXECUTOR_0 = "Custom executor 0";

    /** Executor name for setExecutorConfiguration. */
    private static final String CUSTOM_EXECUTOR_1 = "Custom executor 1";

    /** For storing original value of IGNITE_MBEAN_APPEND_CLASS_LOADER_ID. */
    private String cldProp;

    /** For storing original value of IGNITE_MBEAN_APPEND_JVM_ID. */
    private String jvmIdProp;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cldProp = System.setProperty(IGNITE_MBEAN_APPEND_CLASS_LOADER_ID, "false");

        jvmIdProp = System.setProperty(IGNITE_MBEAN_APPEND_JVM_ID, "false");

        startGrid();
    }

    /** {@inheritDoc} */
    protected void afterTest() throws Exception {
        if (cldProp != null)
            System.setProperty(IGNITE_MBEAN_APPEND_CLASS_LOADER_ID, cldProp);

        if (jvmIdProp != null)
            System.setProperty(IGNITE_MBEAN_APPEND_JVM_ID, jvmIdProp);

        stopGrid();
    }

    /**
     * {@inheritDoc}
     *
     * This implementation registers adds custom executors to the configuration.
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        System.setProperty(IGNITE_MBEAN_APPEND_CLASS_LOADER_ID, "false");

        System.setProperty(IGNITE_MBEAN_APPEND_JVM_ID, "false");

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setExecutorConfiguration(new ExecutorConfiguration(CUSTOM_EXECUTOR_0),
            new ExecutorConfiguration(CUSTOM_EXECUTOR_1));

        return cfg;
    }

    /**
     * Check that kernal bean is available.
     *
     * @throws Exception Thrown if test fails.
     */
    public void testKernalBeans() throws Exception {
        checkBean("Kernal", "IgniteKernal", "InstanceName", grid().name());
        checkBean("Kernal", "ClusterLocalNodeMetricsMXBeanImpl", "TotalServerNodes", 1);
        checkBean("Kernal", "ClusterMetricsMXBeanImpl", "TotalServerNodes", 1);
    }

    /**
     * Check that kernal bean is available.
     *
     * @throws Exception Thrown if test fails.
     */
    public void testExecutorBeans() throws Exception {
        // Standard executors.
        checkBean("Thread Pools", "GridExecutionExecutor", "Terminated", false);
        checkBean("Thread Pools", "GridSystemExecutor", "Terminated", false);
        checkBean("Thread Pools", "GridManagementExecutor", "Terminated", false);
        checkBean("Thread Pools", "GridClassLoadingExecutor", "Terminated", false);
        checkBean("Thread Pools", "GridQueryExecutor", "Terminated", false);
        checkBean("Thread Pools", "GridSchemaExecutor", "Terminated", false);
        checkBean("Thread Pools", "StripedExecutor", "Terminated", false);

        // Custom executors.
        checkBean("Thread Pools", CUSTOM_EXECUTOR_0, "Terminated", false);
        checkBean("Thread Pools", CUSTOM_EXECUTOR_1, "Terminated", false);
    }

    /**
     * Check that kernal beans have expected names.
     *
     * @throws Exception Thrown if test fails.
     */
    public void testIgniteKernalBeansNames() throws Exception {
        // Standard executors.
        checkBeanName("Thread Pools", "GridExecutionExecutor");
        checkBeanName("Thread Pools", "GridSystemExecutor");
        checkBeanName("Thread Pools", "GridManagementExecutor");
        checkBeanName("Thread Pools", "GridClassLoadingExecutor");
        checkBeanName("Thread Pools", "GridQueryExecutor");
        checkBeanName("Thread Pools", "GridSchemaExecutor");
        checkBeanName("Thread Pools", "StripedExecutor");

        // Kernal beans.
        checkBeanName("Kernal", "IgniteKernal");
        checkBeanName("Kernal", "ClusterLocalNodeMetricsMXBeanImpl");
        checkBeanName("Kernal", "ClusterMetricsMXBeanImpl");
    }

    /**
     * Check that kernal beans were successfully registered by their expected names.
     *
     * @throws Exception Thrown if test fails.
     */
    public void testIgniteKernalBeansRegistration() throws Exception {
        // Standard executors.
        checkBeanRegistrationByName("Thread Pools", "GridExecutionExecutor");
        checkBeanRegistrationByName("Thread Pools", "GridSystemExecutor");
        checkBeanRegistrationByName("Thread Pools", "GridManagementExecutor");
        checkBeanRegistrationByName("Thread Pools", "GridClassLoadingExecutor");
        checkBeanRegistrationByName("Thread Pools", "GridQueryExecutor");
        checkBeanRegistrationByName("Thread Pools", "GridSchemaExecutor");
        checkBeanRegistrationByName("Thread Pools", "StripedExecutor");

        // Kernal beans.
        checkBeanRegistrationByName("Kernal", "IgniteKernal");
        checkBeanRegistrationByName("Kernal", "ClusterLocalNodeMetricsMXBeanImpl");
        checkBeanRegistrationByName("Kernal", "ClusterMetricsMXBeanImpl");
    }

    /**
     * Checks that a bean with the specified group and name is available and has the expected attribute.
     *
     * @param grp group name.
     * @param name mbean name.
     * @param attributeName mbean attribute name.
     * @param expAttributeVal expected attribute name.
     * @throws Exception Thrown if test fails.
     */
    private void checkBean(String grp, String name, String attributeName, Object expAttributeVal) throws Exception {
        ObjectName mBeanName = IgniteUtils.makeMBeanName(grid().name(), grp, name);

        Object attributeVal = grid().configuration().getMBeanServer().getAttribute(mBeanName, attributeName);

        assertEquals(expAttributeVal, attributeVal);
    }

    /**
     * Checks that a bean name generation returns the expected value.
     *
     * @param grp group name.
     * @param name mbean name.
     * @throws Exception Thrown if test fails.
     */
    private void checkBeanName(String grp, String name) throws Exception {
        final String EXPECTED_BEAN_NAME = getExpectedBeanName(JMX_DOMAIN, grid().name(), grp, name);

        ObjectName mBeanName = IgniteUtils.makeMBeanName(grid().name(), grp, name);

        assertTrue(EXPECTED_BEAN_NAME.equals(mBeanName.getCanonicalName()));
    }

    /**
     * Checks that a bean name generation returns the expected value.
     *
     * @param grp group name.
     * @param name mbean name.
     * @throws Exception Thrown if test fails.
     */
    private void checkBeanRegistrationByName(String grp, String name) throws Exception {
        final String EXPECTED_BEAN_NAME = getExpectedBeanName(JMX_DOMAIN, grid().name(), grp, name);

        assertTrue(grid().configuration().getMBeanServer().isRegistered(new ObjectName(EXPECTED_BEAN_NAME)));
    }

    /**
     * Generates MBean name that is expected by user.
     *
     * @param domain domain name.
     * @param instanceName instance name.
     * @param grp group name.
     * @param name mbean name.
     * @return generated mbean name.
     */
    private String getExpectedBeanName(String domain, String instanceName, String grp, String name) {
        StringBuffer sb = new StringBuffer();

        sb.append(domain + ":");
        sb.append("group=").append(grp).append(",");
        sb.append("igniteInstanceName=").append(instanceName).append(",");
        sb.append("name=").append(name);

        return sb.toString();
    }
}
