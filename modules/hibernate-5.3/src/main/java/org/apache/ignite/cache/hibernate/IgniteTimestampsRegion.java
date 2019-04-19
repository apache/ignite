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

package org.apache.ignite.cache.hibernate;

import org.apache.ignite.Ignite;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.TimestampsRegion;

/**
 * Implementation of {@link TimestampsRegion}. This region is automatically created when query
 * caching is enabled and it holds most recent updates timestamps to queryable tables.
 * Name of timestamps region is {@link RegionFactory#DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME}.
 */
public class IgniteTimestampsRegion extends IgniteGeneralDataRegion implements TimestampsRegion {
    /**
     * @param factory Region factory.
     * @param regionName Region name.
     * @param cache Region cache.
     */
    public IgniteTimestampsRegion(RegionFactory factory, String regionName, Ignite ignite, HibernateCacheProxy cache) {
        super(factory, regionName, ignite, cache);
    }

}
