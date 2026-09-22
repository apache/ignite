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

package org.apache.ignite.internal.metric;

import java.util.Iterator;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.systemview.MetastorageViewWalker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.systemview.view.FiltrableSystemView;
import org.apache.ignite.spi.systemview.view.MetastorageView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl.DATASTORAGE_METRIC_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.METASTORE_VIEW;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl.DISTRIBUTED_METASTORE_VIEW;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/** Tests for {@link SystemView} for metastorage. */
public class SystemViewMetastorageTest extends SystemViewAbstractTest {
    /** */
    @Test
    public void testMetastorage() throws Exception {
        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(getConfiguration().setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )))) {
            ignite.cluster().state(ClusterState.ACTIVE);

            IgniteCacheDatabaseSharedManager db = ignite.context().cache().context().database();

            SystemView<MetastorageView> metaStoreView = ignite.context().systemView().view(METASTORE_VIEW);

            assertNotNull(metaStoreView);

            String name = "test-key";
            String val = "test-value";
            String unmarshalledName = "unmarshalled-key";
            String unmarshalledVal = "[Raw data. 0 bytes]";

            db.checkpointReadLock();

            try {
                db.metaStorage().write(name, val);
                db.metaStorage().writeRaw(unmarshalledName, new byte[0]);
            }
            finally {
                db.checkpointReadUnlock();
            }

            assertNotNull(F.find(metaStoreView, null,
                (IgnitePredicate<? super MetastorageView>)view ->
                    name.equals(view.name()) && val.equals(view.value())));

            assertNotNull(F.find(metaStoreView, null,
                (IgnitePredicate<? super MetastorageView>)view ->
                    unmarshalledName.equals(view.name()) && unmarshalledVal.equals(view.value())));

            // Test filtering.
            assertTrue(metaStoreView instanceof FiltrableSystemView);

            Iterator<MetastorageView> iter = ((FiltrableSystemView<MetastorageView>)metaStoreView)
                .iterator(F.asMap(MetastorageViewWalker.NAME_FILTER, name));

            assertTrue(iter.hasNext());

            MetastorageView row = iter.next();

            assertTrue(name.equals(row.name()) && val.equals(row.value()));
            assertFalse(iter.hasNext());
        }
    }

    /** */
    @Test
    public void testDistributedMetastorage() throws Exception {
        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(getConfiguration().setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )))) {
            ignite.cluster().state(ClusterState.ACTIVE);

            String histogramName = "CheckpointBeforeLockHistogram";

            ignite.context().metric().configureHistogram(metricName(DATASTORAGE_METRIC_PREFIX, histogramName), new long[] { 1, 2, 3});

            DistributedMetaStorage dms = ignite.context().distributedMetastorage();

            String name = "test-distributed-key";
            String val = "test-distributed-value";

            dms.write(name, val);

            SystemView<MetastorageView> dmsView = ignite.context().systemView().view(DISTRIBUTED_METASTORE_VIEW);

            assertNotNull(F.find(
                dmsView,
                null,
                (IgnitePredicate<? super MetastorageView>)view -> name.equals(view.name()) && val.equals(view.value()))
            );

            assertNotNull(F.find(
                dmsView,
                null,
                (IgnitePredicate<? super MetastorageView>)
                    view -> view.name().endsWith(histogramName) && "[1, 2, 3]".equals(view.value()))
            );

            // Test filtering.
            assertTrue(dmsView instanceof FiltrableSystemView);

            Iterator<MetastorageView> iter = ((FiltrableSystemView<MetastorageView>)dmsView)
                .iterator(F.asMap(MetastorageViewWalker.NAME_FILTER, name));

            assertTrue(iter.hasNext());

            MetastorageView row = iter.next();

            assertTrue(name.equals(row.name()) && val.equals(row.value()));
            assertFalse(iter.hasNext());
        }
    }
}
