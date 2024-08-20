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

package org.apache.ignite.internal.ducktest.tests.thin_client_query_test;

import java.util.LinkedHashMap;
import java.util.List;
import javax.cache.Cache;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.util.typedef.F;

/** Tests cache queries for thin client. */
public class ThinClientQueryTestApplication extends IgniteAwareApplication {
    /** */
    private static final int CNT = 100;

    /** */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        markInitialized();

        QueryEntity qe = new QueryEntity(Integer.class, EntityValue.class)
            .setValueType(EntityValue.class.getName())
            .setFields(new LinkedHashMap<>(F.asMap("val", Integer.class.getName())))
            .setIndexes(F.asList(new QueryIndex("val", false).setName("VAL_IDX")));

        ClientCacheConfiguration clnCacheCfg = new ClientCacheConfiguration()
            .setName("testCache")
            .setQueryEntities(qe);

        ClientCache<Integer, EntityValue> cache = client.createCache(clnCacheCfg);

        for (int i = 0; i < CNT; i++)
            cache.put(i, new EntityValue(i));

        boolean filter = jsonNode.get("filter").asBoolean();

        testIndexQuery(cache, filter);
        testBinaryIndexQuery(cache, filter);
        testScanQuery(cache, filter);
        testBinaryScanQuery(cache, filter);

        markFinished();
    }

    /** */
    private void testIndexQuery(ClientCache<Integer, EntityValue> cache, boolean filter) {
        IndexQuery<Integer, EntityValue> idxQry = new IndexQuery<>(EntityValue.class.getName(), "VAL_IDX");

        if (filter)
            idxQry.setFilter((k, v) -> v.val < CNT / 2);

        List<Cache.Entry<Integer, EntityValue>> result = cache.query(idxQry).getAll();

        int cnt = filter ? CNT / 2 : CNT;

        assert result.size() == cnt;

        for (int i = 0; i < cnt; i++) {
            Cache.Entry<Integer, EntityValue> e = result.get(i);

            assert e.getKey() == i;
            assert e.getValue().val == i;
        }
    }

    /** */
    private void testBinaryIndexQuery(ClientCache<Integer, EntityValue> cache, boolean filter) {
        IndexQuery<Integer, BinaryObject> idxQry = new IndexQuery<>(EntityValue.class.getName(), "VAL_IDX");

        if (filter)
            idxQry.setFilter((k, v) -> (int)v.field("val") < CNT / 2);

        List<Cache.Entry<Integer, BinaryObject>> result = cache.withKeepBinary().query(idxQry).getAll();

        int cnt = filter ? CNT / 2 : CNT;

        assert result.size() == cnt;

        for (int i = 0; i < cnt; i++) {
            Cache.Entry<Integer, BinaryObject> e = result.get(i);

            assert e.getKey() == i;
            assert (int)e.getValue().field("val") == i;
        }
    }

    /** */
    private void testScanQuery(ClientCache<Integer, EntityValue> cache, boolean filter) {
        ScanQuery<Integer, EntityValue> scanQry = new ScanQuery<>();

        if (filter)
            scanQry.setFilter((k, v) -> v.val < CNT / 2);

        List<Cache.Entry<Integer, EntityValue>> result = cache.query(scanQry).getAll();

        int cnt = filter ? CNT / 2 : CNT;

        assert result.size() == cnt;

        for (int i = 0; i < cnt; i++) {
            Cache.Entry<Integer, EntityValue> e = result.get(i);

            assert e.getKey() == e.getValue().val;
        }
    }

    /** */
    private void testBinaryScanQuery(ClientCache<Integer, EntityValue> cache, boolean filter) {
        ScanQuery<Integer, BinaryObject> scanQry = new ScanQuery<>();

        if (filter)
            scanQry.setFilter((k, v) -> (int)v.field("val") < CNT / 2);

        List<Cache.Entry<Integer, BinaryObject>> result = cache.withKeepBinary().query(scanQry).getAll();

        int cnt = filter ? CNT / 2 : CNT;

        assert result.size() == cnt;

        for (int i = 0; i < cnt; i++) {
            Cache.Entry<Integer, BinaryObject> e = result.get(i);

            assert e.getKey() == e.getValue().field("val");
        }
    }

    /** */
    private static class EntityValue {
        /** */
        private final int val;

        /** */
        public EntityValue(int val) {
            this.val = val;
        }

        /** */
        public String toString() {
            return "EntityValue [val=" + val + "]";
        }
    }
}
