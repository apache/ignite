/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.metric;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.GridTopic.TOPIC_METRICS;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 *
 */
public class  MetricProtocolTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration ccfg = defaultCacheConfiguration();
        ccfg.setName("test_cache");
        ccfg.setNearConfiguration(null);
        ccfg.setStatisticsEnabled(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRequestMetrics() throws Exception {
        Ignite ignite = startGrid();

        GridKernalContext ctx = ((IgniteEx) ignite).context();

        CountDownLatch latch = new CountDownLatch(1);

        ctx.io().addMessageListener(TOPIC_METRICS, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof MetricResponse) {
                    MetricResponse res = (MetricResponse)msg;

                    log.info("Time: " + res.timestamp());

                    MetricSchema schema = res.schema();

                    res.processData(schema, new MetricValueConsumer() {
                        @Override public void onBoolean(String name, boolean val) {
                            log.info("Metric value: " + name + " - " + val);
                        }

                        @Override public void onInt(String name, int val) {
                            log.info("Metric value: " + name + " - " + val);
                        }

                        @Override public void onLong(String name, long val) {
                            log.info("Metric value: " + name + " - " + val);
                        }

                        @Override public void onDouble(String name, double val) {
                            log.info("Metric value: " + name + " - " + val);
                        }
                    });

                    latch.countDown();
                }
            }
        });

        MetricRequest req = new MetricRequest(-1);

        ctx.io().sendToGridTopic(((IgniteEx)ignite).localNode().id(), TOPIC_METRICS, req, SYSTEM_POOL);

        latch.await();
    }
}
