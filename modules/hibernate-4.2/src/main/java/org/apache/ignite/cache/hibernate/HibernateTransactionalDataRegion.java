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

package org.apache.ignite.cache.hibernate;

import org.apache.ignite.Ignite;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.TransactionalDataRegion;
import org.hibernate.cache.spi.access.AccessType;

/**
 * Implementation of {@link TransactionalDataRegion} (transactional means that
 * data in the region is updated in connection with database transaction).
 * This interface defines base contract for {@link EntityRegion}, {@link CollectionRegion}
 * and {@link NaturalIdRegion}.
 */
public class HibernateTransactionalDataRegion extends HibernateRegion implements TransactionalDataRegion {
    /** */
    private final CacheDataDescription dataDesc;

    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param ignite Grid.
     * @param cache Region cache.
     * @param dataDesc Region data description.
     */
    HibernateTransactionalDataRegion(HibernateRegionFactory factory, String name,
        Ignite ignite, HibernateCacheProxy cache, CacheDataDescription dataDesc) {
        super(factory, name, ignite, cache);

        this.dataDesc = dataDesc;
    }

    /** {@inheritDoc} */
    @Override public boolean isTransactionAware() {
        return false; // This method is not used by Hibernate.
    }

    /** {@inheritDoc} */
    @Override public CacheDataDescription getCacheDataDescription() {
        return dataDesc;
    }

    /**
     * @param accessType Hibernate L2 cache access type.
     * @return Access strategy for given access type.
     */
    HibernateAccessStrategyAdapter createAccessStrategy(AccessType accessType) {
        switch (accessType) {
            case READ_ONLY:
                return factory.accessStrategyFactory().createReadOnlyStrategy(cache);

            case NONSTRICT_READ_WRITE:
                return factory.accessStrategyFactory().createNonStrictReadWriteStrategy(cache);

            case READ_WRITE:
                return factory.accessStrategyFactory().createReadWriteStrategy(cache);

            case TRANSACTIONAL:
                return factory.accessStrategyFactory().createTransactionalStrategy(cache);

            default:
                throw new IllegalArgumentException("Unknown Hibernate access type: " + accessType);
        }
    }
}
