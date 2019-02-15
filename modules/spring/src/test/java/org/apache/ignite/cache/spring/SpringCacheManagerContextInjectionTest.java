/*
 *                    GridGain Community Edition Licensing
 *                    Copyright 2019 GridGain Systems, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 *  Restriction; you may not use this file except in compliance with the License. You may obtain a
 *  copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 *  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the specific language governing permissions
 *  and limitations under the License.
 *
 *  Commons Clause Restriction
 *
 *  The Software is provided to you by the Licensor under the License, as defined below, subject to
 *  the following condition.
 *
 *  Without limiting other conditions in the License, the grant of rights under the License will not
 *  include, and the License does not grant to you, the right to Sell the Software.
 *  For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 *  under the License to provide to third parties, for a fee or other consideration (including without
 *  limitation fees for hosting or consulting/ support services related to the Software), a product or
 *  service whose value derives, entirely or substantially, from the functionality of the Software.
 *  Any license notice or attribution required by the License must also include this Commons Clause
 *  License Condition notice.
 *
 *  For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 *  the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 *  Edition software provided with this notice.
 */

package org.apache.ignite.cache.spring;

import org.apache.ignite.Ignite;
import org.apache.ignite.TestInjectionLifecycleBean;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *
 */
@RunWith(JUnit4.class)
public class SpringCacheManagerContextInjectionTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBeanInjectionUsingConfigPath() throws Exception {
        new AnnotationConfigApplicationContext(TestPathConfiguration.class);

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
    @Test
    public void testBeanInjectionUsingConfiguration() throws Exception {
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
        public SpringCacheManager springCacheManager() {
            SpringCacheManager mgr = new SpringCacheManager();

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
        public SpringCacheManager springCacheManager() {
            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.setLocalHost("127.0.0.1");

            cfg.setIgniteInstanceName("scmt");

            cfg.setLifecycleBeans(bean1(), bean2());

            SpringCacheManager mgr = new SpringCacheManager();

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
