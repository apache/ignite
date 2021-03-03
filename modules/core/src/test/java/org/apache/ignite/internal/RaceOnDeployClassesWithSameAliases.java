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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * Tets reproduces issue which can happens deploing classes to local store from difference class loaders.
 */
@GridCommonTest(group = "P2P")
public class RaceOnDeployClassesWithSameAliases extends GridCommonAbstractTest {
    /** Listening logger. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(true, log);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        listeningLog.clearListeners();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            .setPeerClassLoadingEnabled(true)
            .setFailureHandler(new StopNodeFailureHandler())
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME));
    }

    /**
     * Test loads class with same alias in time another class deploying.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        setRootLoggerDebugLevel();

        IgniteEx crd = startGrids(1);

        Ignite client = startClientGrid("client");

        awaitPartitionMapExchange();

        AtomicBoolean deployed = new AtomicBoolean();

        LogListener logLsnr = LogListener.matches(logStr -> {
            if (logStr.startsWith("Retrieved auto-loaded resource from spi:") &&
                logStr.contains(TestCacheEntryProcessor.class.getSimpleName())) {

                if (deployed.compareAndSet(false, true)) {
                    System.out.println("dirty getting a breakdown location " + logStr);

                    crd.compute().localDeployTask(TestTask.class, new TestClassLoader());
                }
                return true;
            }

            return false;
        }).build();

        listeningLog.registerListener(logLsnr);

        client.cache(DEFAULT_CACHE_NAME).invoke(1, new TestCacheEntryProcessor());

        assertTrue(logLsnr.check());
    }

    /**
     * Test entry processor.
     */
    private static class TestCacheEntryProcessor implements CacheEntryProcessor<Object, Object, Object> {
        /** {@inheritDoc} */
        @Override public Object process(
            MutableEntry<Object, Object> entry,
            Object... objects
        ) throws EntryProcessorException {
            return 2;
        }
    }

    /**
     * That is a compute task with same aliase as entry processor {@link TestCacheEntryProcessor}.
     */
    @ComputeTaskName("org.apache.ignite.internal.RaceOnDeployClassesWithSameAliases$TestCacheEntryProcessor")
    private static class TestTask extends ComputeTaskAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) {
            assert false;

            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return new Object();
        }
    }

    /**
     * Test {@link ClassLoader}.
     */
    private static class TestClassLoader extends ClassLoader {
        /** {@inheritDoc} */
        @Override protected Class<?> findClass(String name) throws ClassNotFoundException {
            return Thread.currentThread().getContextClassLoader().loadClass(name);
        }
    }
}
