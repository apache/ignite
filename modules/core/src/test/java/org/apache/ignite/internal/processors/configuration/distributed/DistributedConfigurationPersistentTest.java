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

package org.apache.ignite.internal.processors.configuration.distributed;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty.detachedLongProperty;
import static org.junit.Assume.assumeTrue;

/** */
public class DistributedConfigurationPersistentTest extends DistributedConfigurationAbstractTest {
    /** {@inheritDoc} */
    @Override protected boolean isPersistent() {
        return true;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSuccessClusterWideUpdate() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        DistributedProperty<Long> long0 = distr(ignite0).registerProperty(detachedLongProperty(TEST_PROP));
        DistributedProperty<Long> long1 = distr(ignite1).registerProperty(detachedLongProperty(TEST_PROP));

        long0.propagate(0L);

        assertEquals(0, long0.get().longValue());
        assertEquals(0, long1.get().longValue());

        assertTrue(long0.propagate(2L));

        //Value changed on whole grid.
        assertEquals(2L, long0.get().longValue());
        assertEquals(2L, long1.get().longValue());

        stopAllGrids();

        ignite0 = startGrid(0);
        ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        long0 = distr(ignite0).registerProperty(detachedLongProperty(TEST_PROP));
        long1 = distr(ignite1).registerProperty(detachedLongProperty(TEST_PROP));

        assertEquals(2, long0.get().longValue());
        assertEquals(2, long1.get().longValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadLocalValueOnInactiveGrid() throws Exception {
        assumeTrue(isPersistent());

        IgniteEx ignite0 = startGrid(0);
        startGrid(1);

        ignite0.cluster().active(true);

        DistributedProperty<Long> long0 = distr(ignite0).registerProperty(detachedLongProperty(TEST_PROP));

        long0.propagate(0L);

        assertEquals(0, long0.get().longValue());

        assertTrue(long0.propagate(2L));

        stopAllGrids();

        ignite0 = startGrid(0);

        long0 = distr(ignite0).registerProperty(detachedLongProperty(TEST_PROP));

        assertEquals(2, long0.get().longValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPropagateValueOnInactiveGridShouldNotThrowException() throws Exception {
        assumeTrue(isPersistent());

        IgniteEx ignite0 = (IgniteEx)startGrids(2);

        DistributedProperty<Long> long0 = distr(ignite0).registerProperty(detachedLongProperty(TEST_PROP));

        long0.propagate(2L);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRegisterPropertyBeforeOnReadyForReadHappened() throws Exception {
        assumeTrue(isPersistent());

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        DistributedProperty<Long> long0 = distr(ignite0).registerProperty(detachedLongProperty(TEST_PROP));

        long0.propagate(0L);

        assertEquals(0, long0.get().longValue());

        long0.propagate(2L);

        stopAllGrids();

        AtomicReference<DistributedProperty<Long>> holder = new AtomicReference<>();

        TestDistibutedConfigurationPlugin.supplier = (ctx) -> {
            if (holder.get() == null)
                holder.set(ctx.distributedConfiguration().registerProperty(detachedLongProperty(TEST_PROP)));
        };

        ignite0 = startGrid(0);
        ignite1 = startGrid(1);

        long0 = holder.get();
        DistributedProperty<Long> long1 = distr(ignite1).registerProperty(detachedLongProperty(TEST_PROP));

        //After start it should read from local storage.
        assertEquals(2, long0.get().longValue());
        assertEquals(2, long1.get().longValue());
    }
}
