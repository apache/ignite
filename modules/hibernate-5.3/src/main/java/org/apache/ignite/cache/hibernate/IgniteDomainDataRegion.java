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
import org.hibernate.cache.CacheException;
import org.hibernate.cache.cfg.spi.CollectionDataCachingConfig;
import org.hibernate.cache.cfg.spi.DomainDataRegionBuildingContext;
import org.hibernate.cache.cfg.spi.DomainDataRegionConfig;
import org.hibernate.cache.cfg.spi.EntityDataCachingConfig;
import org.hibernate.cache.cfg.spi.NaturalIdDataCachingConfig;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.ExtendedStatisticsSupport;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.cache.spi.support.AbstractDomainDataRegion;

public class IgniteDomainDataRegion extends AbstractDomainDataRegion implements ExtendedStatisticsSupport {

    private final HibernateCacheProxy cache;
    private HibernateAccessStrategyFactory strategyFactory;

    public IgniteDomainDataRegion(DomainDataRegionConfig regionConfig,
                                  RegionFactory regionFactory,
                                  CacheKeysFactory defaultKeysFactory,
                                  DomainDataRegionBuildingContext buildingContext,
                                  HibernateAccessStrategyFactory strategyFactory) {
        super(regionConfig, regionFactory, defaultKeysFactory, buildingContext);
        this.strategyFactory = strategyFactory;
        this.cache = strategyFactory.regionCache(getName());
        super.completeInstantiation(regionConfig, buildingContext);
    }

    @Override
    protected EntityDataAccess generateEntityAccess(EntityDataCachingConfig entityAccessConfig) {
        AccessType accessType = entityAccessConfig.getAccessType();
        Ignite ignite = strategyFactory.node();
        switch (accessType) {
            case READ_ONLY:
                HibernateAccessStrategyAdapter readOnlyStrategy =
                    strategyFactory.createReadOnlyStrategy(cache);
                return new IgniteEntityDataAccess(readOnlyStrategy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case NONSTRICT_READ_WRITE:
                HibernateAccessStrategyAdapter nonStrictReadWriteStrategy =
                    strategyFactory.createNonStrictReadWriteStrategy(cache);
                return new IgniteEntityDataAccess(nonStrictReadWriteStrategy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case READ_WRITE:
                HibernateAccessStrategyAdapter readWriteStrategy =
                    strategyFactory.createReadWriteStrategy(cache);
                return new IgniteEntityDataAccess(readWriteStrategy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case TRANSACTIONAL:
                HibernateAccessStrategyAdapter transactionalStrategy =
                    strategyFactory.createTransactionalStrategy(cache);
                return new IgniteEntityDataAccess(transactionalStrategy, accessType, getRegionFactory(),
                    this, ignite, cache);

            default:
                throw new IllegalArgumentException("Unknown Hibernate access type: " + accessType);
        }
    }

    @Override
    protected CollectionDataAccess generateCollectionAccess(CollectionDataCachingConfig cachingConfig) {
        HibernateCacheProxy cache = strategyFactory.regionCache(getName());
        AccessType accessType = cachingConfig.getAccessType();
        Ignite ignite = strategyFactory.node();
        switch (accessType) {
            case READ_ONLY:
                HibernateAccessStrategyAdapter readOnlyStrategy =
                    strategyFactory.createReadOnlyStrategy(cache);
                return new IgniteCollectionDataAccess(readOnlyStrategy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case NONSTRICT_READ_WRITE:
                HibernateAccessStrategyAdapter nonStrictReadWriteStrategy =
                    strategyFactory.createNonStrictReadWriteStrategy(cache);
                return new IgniteCollectionDataAccess(nonStrictReadWriteStrategy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case READ_WRITE:
                HibernateAccessStrategyAdapter readWriteStrategy =
                    strategyFactory.createReadWriteStrategy(cache);
                return new IgniteCollectionDataAccess(readWriteStrategy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case TRANSACTIONAL:
                HibernateAccessStrategyAdapter transactionalStrategy =
                    strategyFactory.createTransactionalStrategy(cache);
                return new IgniteCollectionDataAccess(transactionalStrategy, accessType, getRegionFactory(),
                    this, ignite, cache);

            default:
                throw new IllegalArgumentException("Unknown Hibernate access type: " + accessType);
        }
    }

    @Override
    protected NaturalIdDataAccess generateNaturalIdAccess(NaturalIdDataCachingConfig naturalIdAccessConfig) {
        HibernateCacheProxy cache = strategyFactory.regionCache(getName());
        AccessType accessType = naturalIdAccessConfig.getAccessType();
        Ignite ignite = strategyFactory.node();
        switch (accessType) {
            case READ_ONLY:
                HibernateAccessStrategyAdapter readOnlyStrategy =
                    strategyFactory.createReadOnlyStrategy(cache);
                return new IgniteNaturalIdDataAccess(readOnlyStrategy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case NONSTRICT_READ_WRITE:
                HibernateAccessStrategyAdapter nonStrictReadWriteStrategy =
                    strategyFactory.createNonStrictReadWriteStrategy(cache);
                return new IgniteNaturalIdDataAccess(nonStrictReadWriteStrategy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case READ_WRITE:
                HibernateAccessStrategyAdapter readWriteStrategy =
                    strategyFactory.createReadWriteStrategy(cache);
                return new IgniteNaturalIdDataAccess(readWriteStrategy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case TRANSACTIONAL:
                HibernateAccessStrategyAdapter transactionalStrategy =
                    strategyFactory.createTransactionalStrategy(cache);
                return new IgniteNaturalIdDataAccess(transactionalStrategy, accessType, getRegionFactory(),
                    this, ignite, cache);

            default:
                throw new IllegalArgumentException("Unknown Hibernate access type: " + accessType);
        }
    }

    @Override
    public void destroy() throws CacheException {
        // no-op
    }

    @Override
    public long getElementCountInMemory() {
        return cache.offHeapEntriesCount();
    }

    @Override
    public long getElementCountOnDisk() {
        return cache.sizeLong();
    }

    @Override
    public long getSizeInMemory() {
        return cache.offHeapAllocatedSize();
    }
}
