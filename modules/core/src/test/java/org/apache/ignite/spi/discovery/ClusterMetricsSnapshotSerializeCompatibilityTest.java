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

package org.apache.ignite.spi.discovery;

import java.util.HashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.processors.cache.CacheMetricsSnapshot;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class ClusterMetricsSnapshotSerializeCompatibilityTest extends GridCommonAbstractTest {
    /** */
    public ClusterMetricsSnapshotSerializeCompatibilityTest() {
        super(false /*don't start grid*/);
    }

    /** Marshaller. */
    private OptimizedMarshaller marshaller = marshaller();

    /** */
    @Test
    public void testSerializationAndDeserialization() throws IgniteCheckedException {
        HashMap<Integer, CacheMetricsSnapshot> metrics = new HashMap<>();

        metrics.put(1, createMetricsWithBorderMarker());

        metrics.put(2, createMetricsWithBorderMarker());

        byte[] zipMarshal = U.zip(U.marshal(marshaller, metrics));

        HashMap<Integer, CacheMetricsSnapshot> unmarshalMetrics = U.unmarshalZip(marshaller, zipMarshal, null);

        assertTrue(isMetricsEquals(metrics.get(1), unmarshalMetrics.get(1)));

        assertTrue(isMetricsEquals(metrics.get(2), unmarshalMetrics.get(2)));
    }

    /**
     * @return Test metrics.
     */
    private CacheMetricsSnapshot createMetricsWithBorderMarker() {
        CacheMetricsSnapshot metrics = new CacheMetricsSnapshot();

        GridTestUtils.setFieldValue(metrics, "rebalancingKeysRate", 1234);
        GridTestUtils.setFieldValue(metrics, "reads", 3232);

        return metrics;
    }

    /**
     * @param obj Object.
     * @param obj1 Object 1.
     */
    private boolean isMetricsEquals(CacheMetricsSnapshot obj, CacheMetricsSnapshot obj1) {
        return obj.getRebalancingKeysRate() == obj1.getRebalancingKeysRate() && obj.getCacheGets() == obj1.getCacheGets();
    }

    /**
     * @return Marshaller.
     */
    private OptimizedMarshaller marshaller() {
        U.clearClassCache();

        OptimizedMarshaller marsh = new OptimizedMarshaller();

        marsh.setContext(new MarshallerContextTestImpl());

        return marsh;
    }
}
