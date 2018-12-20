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
import org.hibernate.cache.spi.DomainDataRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.collection.CollectionPersister;

/** */
public class IgniteCollectionDataAccess extends IgniteCachedDomainDataAccess implements CollectionDataAccess {
    /** */
    private final AccessType accessType;

    /** */
    public IgniteCollectionDataAccess(HibernateAccessStrategyAdapter stgy, AccessType accessType,
        RegionFactory regionFactory,
        DomainDataRegion domainDataRegion, Ignite ignite,
        HibernateCacheProxy cache) {
        super(stgy, regionFactory, domainDataRegion, ignite, cache);

        this.accessType = accessType;
    }

    /** */
    @Override public AccessType getAccessType() {
        return accessType;
    }

    /** */
    @Override public Object generateCacheKey(
        Object id,
        CollectionPersister persister,
        SessionFactoryImplementor factory,
        String tenantIdentifier) {
        return HibernateKeyWrapper.staticCreateCollectionKey(id, persister, tenantIdentifier);
    }

    /** */
    @Override public Object getCacheKeyId(Object cacheKey) {
        return ((HibernateKeyWrapper)cacheKey).id();
    }
}
