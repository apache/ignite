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


package org.apache.ignite.internal.processors.cache;

import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.junit.Test;

/**
 * Checks whether storing to local store doesn't cause binary objects unmarshalling,
 * and as a consequence {@link ClassNotFoundException} to be thrown.
 *
 * @see <a href="https://issues.apache.org/jira/browse/IGNITE-2753">
 *     https://issues.apache.org/jira/browse/IGNITE-2753
 *     </a>
 */
public class GridLocalCacheStoreManagerDeserializationTest extends GridCacheStoreManagerDeserializationTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.LOCAL;
    }

    /**
     * Checks no additional unmarshalling happens in calling
     * {@link GridCacheMapEntry#innerUpdateLocal(GridCacheVersion, GridCacheOperation, Object, Object[],
     * boolean, boolean, boolean, boolean, ExpiryPolicy, boolean, boolean, CacheEntryPredicate[],
     * boolean, UUID, String)}.
     *
     * @throws Exception
     */
    @Test
    public void testUpdate() throws Exception {
        // Goal is to check correct saving to store (no exception must be thrown)

        final Ignite grid = startGrid();

        final IgniteCache<TestObj, TestObj> cache = grid.createCache(CACHE_NAME);

        final TestObj testObj = new TestObj(0);

        cache.put(testObj, testObj);

        assert testObj.equals(cache.get(testObj));
        assert store.map.containsKey(testObj);

        cache.remove(testObj);

        assert cache.get(testObj) == null;
        assert !store.map.containsKey(testObj);
    }

    /**
     * Checks no additional unmarshalling happens in calling
     * {@link GridCacheMapEntry#innerUpdateLocal(GridCacheVersion, GridCacheOperation, Object, Object[],
     * boolean, boolean, boolean, boolean, ExpiryPolicy, boolean, boolean, CacheEntryPredicate[],
     * boolean, UUID, String)} for binary objects.
     *
     * @throws Exception
     */
    @Test
    public void testBinaryUpdate() throws Exception {
        // Goal is to check correct saving to store (no exception must be thrown)
        final Ignite grid = startGrid("binaryGrid");

        final IgniteCache<BinaryObject, BinaryObject> cache = grid.createCache(CACHE_NAME).withKeepBinary();

        final BinaryObjectBuilder builder = grid.binary().builder("custom_type");

        final BinaryObject entity = builder.setField("id", 0).build();

        cache.put(entity, entity);

        assert entity.equals(cache.get(entity));
        assert store.map.containsKey(entity);

        cache.remove(entity);

        assert cache.get(entity) == null;
        assert !store.map.containsKey(entity);
    }
}
