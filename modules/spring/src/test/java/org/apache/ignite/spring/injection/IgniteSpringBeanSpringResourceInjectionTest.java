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

package org.apache.ignite.spring.injection;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Test checking injections of {@link SpringResource} annotated fields.
 */
public class IgniteSpringBeanSpringResourceInjectionTest extends GridCommonAbstractTest {
    /** */
    private static final String SPRING_CFG_LOCATION = "/org/apache/ignite/spring/injection/spring-bean.xml";

    /** */
    private static final String BEAN_TO_INJECT_NAME = "beanToInject";

    /**
     * Cache store with {@link SpringResource} fields to be injected.
     */
    public static class IgniteCacheStoreWithSpringResource<K, V> extends CacheStoreAdapter<K, V>
        implements Serializable
    {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SpringResource(resourceClass = Integer.class)
        private transient Integer injectedSpringFld;

        /**
         * @return Injected Spring field.
         */
        public Integer getInjectedSpringField() {
            return injectedSpringFld;
        }

        /** {@inheritDoc} */
        @Override public V load(K key) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }

    /**
     * Interface of a service with {@link SpringResource} fields to be injected.
     */
    public interface ServiceWithSpringResource {
        /**
         * @return Injected Spring field.
         */
        Integer getInjectedSpringField();
    }

    /**
     * Service with {@link SpringResource} fields to be injected.
     */
    public static class ServiceWithSpringResourceImpl implements ServiceWithSpringResource, Service {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SpringResource(resourceClass = Integer.class)
        private transient Integer injectedSpringFld;

        /** {@inheritDoc} */
        @Override public Integer getInjectedSpringField() {
            return injectedSpringFld;
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }

    /**
     *
     */
    private abstract static class TestSpringResourceInjectedRunnable implements Runnable {
        /** */
        private final String springCfgLocation;

        /** */
        private final String beanToInjectName;

        /** */
        protected BeanFactory appCtx;

        /**
         * Constructor.
         *
         * @param springCfgLocation Spring config location.
         * @param beanToInjectName Bean to inject name.
         */
        protected TestSpringResourceInjectedRunnable(String springCfgLocation, String beanToInjectName) {
            this.springCfgLocation = springCfgLocation;
            this.beanToInjectName = beanToInjectName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            appCtx = new ClassPathXmlApplicationContext(springCfgLocation);

            Integer beanToInject = (Integer)appCtx.getBean(beanToInjectName);

            assertEquals(beanToInject, getInjectedBean());
        }

        /**
         * @return Injected bean to check.
         */
        abstract Integer getInjectedBean();
    }

    /** */
    private void doTestSpringResourceInjected(Runnable testRunnable) throws Exception {
        ExecutorService executorSvc = Executors.newSingleThreadExecutor();

        Future<?> fut = executorSvc.submit(testRunnable);

        try {
            fut.get(5, TimeUnit.SECONDS);
        }
        catch (TimeoutException ignored) {
            fail("Failed to wait for completion. Deadlock is possible");
        }
        finally {
            executorSvc.shutdownNow();
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /** */
    @Test
    public void testSpringResourceInjectedInCacheStore() throws Exception {
        doTestSpringResourceInjected(
            new TestSpringResourceInjectedRunnable(SPRING_CFG_LOCATION, BEAN_TO_INJECT_NAME) {
                /** {@inheritDoc} */
                @Override Integer getInjectedBean() {
                    IgniteCacheStoreWithSpringResource cacheStore = (IgniteCacheStoreWithSpringResource)
                        ((IgniteEx) G.allGrids().get(0)).cachex("cache1").context().store().store();

                    return cacheStore.getInjectedSpringField();
                }
            }
        );
    }

    /** */
    @Test
    public void testSpringResourceInjectedInService() throws Exception {
        doTestSpringResourceInjected(
            new TestSpringResourceInjectedRunnable(SPRING_CFG_LOCATION, BEAN_TO_INJECT_NAME) {
                /** {@inheritDoc} */
                @Override Integer getInjectedBean() {
                    Ignite ignite = appCtx.getBean(Ignite.class);
                    ServiceWithSpringResource svc = ignite.services().service("ServiceWithSpringResource");

                    return svc.getInjectedSpringField();
                }
            }
        );
    }
}
