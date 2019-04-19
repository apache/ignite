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
import org.hibernate.cache.spi.TimestampsRegion;

/**
 * Implementation of {@link TimestampsRegion}. This region is automatically created when query
 * caching is enabled and it holds most recent updates timestamps to queryable tables.
 * Name of timestamps region is {@code "org.hibernate.cache.spi.UpdateTimestampsCache"}.
 */
public class HibernateTimestampsRegion extends HibernateGeneralDataRegion implements TimestampsRegion {
    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param ignite Grid.
     * @param cache Region cache.
     */
    public HibernateTimestampsRegion(HibernateRegionFactory factory, String name,
        Ignite ignite,  HibernateCacheProxy cache) {
        super(factory, name, ignite, cache);
    }
}