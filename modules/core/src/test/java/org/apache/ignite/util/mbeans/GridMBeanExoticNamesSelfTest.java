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

import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Testing registration of MBeans with special characters in group name or bean name.
 */
public class GridMBeanExoticNamesSelfTest extends GridCommonAbstractTest {
    /** Test registration of a bean with special characters in group name. */
    @Test
    public void testGroupWithSpecialSymbols() throws Exception {
        checkMBeanRegistration("dummy!@#$^&*()?\\grp", "dummy");
    }

    /** Test registration of a bean with special characters in name. */
    @Test
    public void testNameWithSpecialSymbols() throws Exception {
        checkMBeanRegistration("dummygrp", "dum!@#$^&*()?\\my");
    }

    /** Test MBean registration. */
    private void checkMBeanRegistration(String grp, String name) throws Exception {
        // Node should start and stop with no errors.
        try (Ignite ignite = startGrid(0)) {
            MBeanServer srv = ignite.configuration().getMBeanServer();

            U.registerMBean(srv, ignite.name(), grp, name, new DummyMBeanImpl(), DummyMBean.class);

            ObjectName objName = U.makeMBeanName(ignite.name(), grp, name + '2');
            U.registerMBean(srv, objName, new DummyMBeanImpl(), DummyMBean.class);
        }
    }

    /**
     * MBean dummy interface.
     */
    public interface DummyMBean {
        /** */
        void noop();
    }

    /**
     * MBean stub.
     */
    private static class DummyMBeanImpl implements DummyMBean {
        /** {@inheritDoc} */
        @Override public void noop() {
            // No op.
        }
    }
}
