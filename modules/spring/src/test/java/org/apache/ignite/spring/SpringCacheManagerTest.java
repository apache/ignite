package org.apache.ignite.spring;

import org.apache.ignite.TestInjectionLifecycleBean;
import org.apache.ignite.cache.spring.SpringCacheManager;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.junit.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Spring cache manager test */
public class SpringCacheManagerTest {
    /** @throws Exception If failed. */
    @Test
    public void testBeanInjection() throws Exception {
        BeanFactory factory = new AnnotationConfigApplicationContext(TestConfiguration.class);

        TestInjectionLifecycleBean bean1 = (TestInjectionLifecycleBean)factory.getBean("bean1");
        TestInjectionLifecycleBean bean2 = (TestInjectionLifecycleBean)factory.getBean("bean2");

        bean1.checkState();
        bean2.checkState();
    }

    /** */
    @SuppressWarnings("WeakerAccess")
    @Configuration
    static class TestConfiguration {

        /** */
        @Bean(name = "mgr")
        public SpringCacheManager springCacheManager() {
            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.setGridName("scmt");

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
