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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
public class PartitionsEvictionTaskFailureHandlerTest extends PartitionsEvictManagerAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testEvictionTaskShouldCallFailureHandler() throws Exception {
        IgniteEx node = startGrid(0);

        AtomicBoolean once = new AtomicBoolean();

        // Partition eviction task should throw exception after completion.
        instrumentEvictionQueue(node, task -> {
            if (!(task instanceof PartitionsEvictManager.PartitionEvictionTask))
                return task;

            // Fail once.
            if (!once.compareAndSet(false, true))
                return task;

            try {
                Field field = U.findField(PartitionsEvictManager.PartitionEvictionTask.class, "finishFut");

                field.setAccessible(true);

                Field modifiersField = Field.class.getDeclaredField("modifiers");
                modifiersField.setAccessible(true);
                modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

                field.set(task, new GridFutureAdapter<Object>() {
                    @Override protected boolean onDone(@Nullable Object res, @Nullable Throwable err, boolean cancel) {
                        if (err == null)
                            throw new RuntimeException("TEST");

                        return super.onDone(res, err, cancel);
                    }
                });
            }
            catch (Exception e) {
                fail();
            }

            return task;
        });

        node.cluster().active(true);

        try (IgniteDataStreamer<Object, Object> streamer = node.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int k = 0; k < 1024; k++)
                node.cache(DEFAULT_CACHE_NAME).put(k, k);
        }

        // Some partitions from node 0 should be evicted.
        startGrid(1);

        assertTrue(GridTestUtils.waitForCondition(() -> failure.get(), 10_000));
    }
}
