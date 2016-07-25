/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.transactions.spring;

import org.apache.ignite.Ignite;
import org.apache.ignite.TestInjectionLifecycleBean;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *
 */
public class SpringTransactionManagerContextInjectionTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testBeanInjectionUsingConfigPath() throws Exception {
        BeanFactory factory = new AnnotationConfigApplicationContext(TestPathConfiguration.class);

        Ignite grid = IgnitionEx.grid("springInjectionTest");

        IgniteConfiguration cfg = grid.configuration();

        LifecycleBean[] beans = cfg.getLifecycleBeans();

        assertEquals(2, beans.length);

        TestInjectionLifecycleBean bean1 = (TestInjectionLifecycleBean)beans[0];
        TestInjectionLifecycleBean bean2 = (TestInjectionLifecycleBean)beans[1];

        bean1.checkState();
        bean2.checkState();
    }

    /**
     * @throws Exception If failed.
     */
    public void testBeanInjectionUsingConfig() throws Exception {
        BeanFactory factory = new AnnotationConfigApplicationContext(TestCfgConfiguration.class);

        TestInjectionLifecycleBean bean1 = (TestInjectionLifecycleBean)factory.getBean("bean1");
        TestInjectionLifecycleBean bean2 = (TestInjectionLifecycleBean)factory.getBean("bean2");

        bean1.checkState();
        bean2.checkState();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @SuppressWarnings("WeakerAccess")
    @Configuration
    static class TestPathConfiguration {
        /** */
        @Bean(name = "mgr")
        public SpringTransactionManager springTransactionManager() {
            SpringTransactionManager mgr = new SpringTransactionManager();

            mgr.setConfigurationPath("org/apache/ignite/spring-injection-test.xml");

            return mgr;
        }
    }

    /** */
    @SuppressWarnings("WeakerAccess")
    @Configuration
    static class TestCfgConfiguration {
        /** */
        @Bean(name = "mgr")
        public SpringTransactionManager springTransactionManager() {
            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.setLocalHost("127.0.0.1");

            cfg.setGridName("stmcit");

            cfg.setLifecycleBeans(bean1(), bean2());

            SpringTransactionManager mgr = new SpringTransactionManager();

            mgr.setConfiguration(cfg);

            return mgr;
        }

        /** */
        @Bean(name = "bean1")
        LifecycleBean bean1() {
            return new TestInjectionLifecycleBean();
        }

        /** */
        @Bean(name = "bean2")
        LifecycleBean bean2() {
            return new TestInjectionLifecycleBean();
        }
    }
}