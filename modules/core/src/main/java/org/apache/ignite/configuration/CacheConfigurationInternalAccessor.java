/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration;

import java.util.Collection;
import org.apache.ignite.cache.QueryEntity;

/** Provides internal access to {@link CacheConfiguration} implementation details. */
public final class CacheConfigurationInternalAccessor {
    /** */
    private CacheConfigurationInternalAccessor() {
        // No-op.
    }

    /**
     * Replaces query entities without changing the query-entity configuration source.
     *
     * @param cfg Cache configuration.
     * @param qryEntities Query entities.
     */
    public static void replaceQueryEntities(CacheConfiguration<?, ?> cfg, Collection<QueryEntity> qryEntities) {
        cfg.replaceQueryEntities(qryEntities);
    }
}
