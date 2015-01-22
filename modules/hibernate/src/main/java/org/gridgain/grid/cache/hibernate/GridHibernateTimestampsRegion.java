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

package org.gridgain.grid.cache.hibernate;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.hibernate.cache.spi.*;

/**
 * Implementation of {@link TimestampsRegion}. This region is automatically created when query
 * caching is enabled and it holds most recent updates timestamps to queryable tables.
 * Name of timestamps region is {@code "org.hibernate.cache.spi.UpdateTimestampsCache"}.
 */
public class GridHibernateTimestampsRegion extends GridHibernateGeneralDataRegion implements TimestampsRegion {
    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param ignite Grid.
     * @param cache Region cache.
     */
    public GridHibernateTimestampsRegion(GridHibernateRegionFactory factory, String name,
        Ignite ignite, GridCache<Object, Object> cache) {
        super(factory, name, ignite, cache);
    }
}
