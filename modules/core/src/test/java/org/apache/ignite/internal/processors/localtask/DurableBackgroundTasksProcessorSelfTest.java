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

package org.apache.ignite.internal.processors.localtask;

import java.util.Map;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Class for testing the {@link DurableBackgroundTasksProcessor}.
 */
public class DurableBackgroundTasksProcessorSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    @Test
    public void test0() throws Exception {
        IgniteEx n = startGrid(0);

        // TODO: 26.04.2021 write test
    }

    /**
     * Asynchronous execution of a durable background task.
     *
     * @param n Node.
     * @param t Task.
     * @param save Save task to MetaStorage.
     * @return Task future.
     */
    private IgniteInternalFuture<Void> execAsync(IgniteEx n, DurableBackgroundTask t, boolean save) {
        return durableBackgroundTasksProcessor(n).executeAsync(t, save);
    }

    /**
     * Getting {@code DurableBackgroundTasksProcessor#tasks}.
     *
     * @param n Node.
     * @return Task states map.
     */
    private Map<String, DurableBackgroundTaskState> tasks(IgniteEx n) {
        return getFieldValue(durableBackgroundTasksProcessor(n), "tasks");
    }

    /**
     * Getting durable background task processor.
     *
     * @param n Node.
     * @return Durable background task processor.
     */
    private DurableBackgroundTasksProcessor durableBackgroundTasksProcessor(IgniteEx n) {
        return n.context().durableBackgroundTasksProcessor();
    }

    /**
     * Performing an operation on a MetaStorage.
     *
     * @param n Node.
     * @param fun Function for working with MetaStorage, the argument can be {@code null}.
     * @return The function result.
     */
    private <R> R metaStorageOperation(IgniteEx n, IgniteThrowableFunction<MetaStorage, R> fun) throws Exception {
        IgniteCacheDatabaseSharedManager db = n.context().cache().context().database();

        db.checkpointReadLock();

        try {
            return fun.apply(db.metaStorage());
        }
        finally {
            db.checkpointReadUnlock();
        }
    }
}
