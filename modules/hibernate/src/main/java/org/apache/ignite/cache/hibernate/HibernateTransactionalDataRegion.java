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
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.TransactionalDataRegion;
import org.hibernate.cache.spi.access.AccessType;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

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
    public HibernateTransactionalDataRegion(HibernateRegionFactory factory, String name,
        Ignite ignite, IgniteInternalCache<Object, Object> cache, CacheDataDescription dataDesc) {
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
    protected HibernateAccessStrategyAdapter createAccessStrategy(AccessType accessType) {
        switch (accessType) {
            case READ_ONLY:
                return new HibernateReadOnlyAccessStrategy(ignite, cache);

            case NONSTRICT_READ_WRITE:
                return new HibernateNonStrictAccessStrategy(ignite, cache, factory.threadLocalForCache(cache.name()));

            case READ_WRITE:
                if (cache.configuration().getAtomicityMode() != TRANSACTIONAL)
                    throw new CacheException("Hibernate READ-WRITE access strategy must have Ignite cache with " +
                        "'TRANSACTIONAL' atomicity mode: " + cache.name());

                return new HibernateReadWriteAccessStrategy(ignite, cache, factory.threadLocalForCache(cache.name()));

            case TRANSACTIONAL:
                if (cache.configuration().getAtomicityMode() != TRANSACTIONAL)
                    throw new CacheException("Hibernate TRANSACTIONAL access strategy must have Ignite cache with " +
                        "'TRANSACTIONAL' atomicity mode: " + cache.name());

                if (cache.configuration().getTransactionManagerLookupClassName() == null) {
                    TransactionConfiguration txCfg = ignite.configuration().getTransactionConfiguration();
                    
                    if (txCfg == null || txCfg.getTxManagerLookupClassName() == null)
                        throw new CacheException("Hibernate TRANSACTIONAL access strategy must have Ignite with " +
                            "TransactionManagerLookup configured (see IgniteConfiguration." +
                            "getTransactionConfiguration().getTxManagerLookupClassName()): " + cache.name());
                }

                return new HibernateTransactionalAccessStrategy(ignite, cache);

            default:
                throw new IllegalArgumentException("Unknown Hibernate access type: " + accessType);
        }
    }
}