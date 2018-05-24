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

import java.util.Properties;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.cache.spi.access.AccessType;

import static org.apache.ignite.cache.hibernate.HibernateAccessStrategyFactory.DFLT_ACCESS_TYPE_PROPERTY;
import static org.hibernate.cache.spi.access.AccessType.NONSTRICT_READ_WRITE;

/**
 * Hibernate L2 cache region factory.
 * <p>
 * Following Hibernate settings should be specified to enable second level cache and to use this
 * region factory for caching:
 * <pre name="code" class="brush: xml; gutter: false;">
 * hibernate.cache.use_second_level_cache=true
 * hibernate.cache.region.factory_class=org.apache.ignite.cache.hibernate.HibernateRegionFactory
 * </pre>
 * Note that before region factory is started you need to start properly configured Ignite node in the same JVM.
 * For example to start Ignite node one of loader provided in {@code org.apache.ignite.grid.startup} package can be used.
 * <p>
 * Name of Ignite instance to be used for region factory must be specified as following Hibernate property:
 * <pre name="code" class="brush: xml; gutter: false;">
 * org.apache.ignite.hibernate.ignite_instance_name=&lt;Ignite instance name&gt;
 * </pre>
 * Each Hibernate cache region must be associated with some {@link IgniteInternalCache}, by default it is assumed that
 * for each cache region there is a {@link IgniteInternalCache} with the same name. Also it is possible to define
 * region to cache mapping using properties with prefix {@code org.apache.ignite.hibernate.region_cache}.
 * For example if for region with name "region1" cache with name "cache1" should be used then following
 * Hibernate property should be specified:
 * <pre name="code" class="brush: xml; gutter: false;">
 * org.apache.ignite.hibernate.region_cache.region1=cache1
 * </pre>
 */
public class HibernateRegionFactory implements RegionFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    static final HibernateExceptionConverter EXCEPTION_CONVERTER = new HibernateExceptionConverter() {
        @Override public RuntimeException convert(Exception e) {
            return new CacheException(e);
        }
    };

    /** Default region access type. */
    private AccessType dfltAccessType;

    /** Key transformer. */
    private final HibernateKeyTransformer hibernate4transformer = new HibernateKeyTransformer() {
        @Override public Object transform(Object key) {
            return key;
        }
    };

    /** */
    private final HibernateAccessStrategyFactory accessStgyFactory =
        new HibernateAccessStrategyFactory(hibernate4transformer, EXCEPTION_CONVERTER);

    /** {@inheritDoc} */
    @Override public void start(SessionFactoryOptions settings, Properties props) throws CacheException {
        String accessType = props.getProperty(DFLT_ACCESS_TYPE_PROPERTY, NONSTRICT_READ_WRITE.name());

        dfltAccessType = AccessType.valueOf(accessType);

        accessStgyFactory.start(props);
    }

    /**
     * @return Access strategy factory.
     */
    HibernateAccessStrategyFactory accessStrategyFactory() {
        return accessStgyFactory;
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isMinimalPutsEnabledByDefault() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public AccessType getDefaultAccessType() {
        return dfltAccessType;
    }

    /** {@inheritDoc} */
    @Override public long nextTimestamp() {
        return System.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public EntityRegion buildEntityRegion(String regionName, Properties props, CacheDataDescription metadata)
        throws CacheException {
        return new HibernateEntityRegion(this,
            regionName,
            accessStgyFactory.node(),
            accessStgyFactory.regionCache(regionName),
            metadata);
    }

    /** {@inheritDoc} */
    @Override public NaturalIdRegion buildNaturalIdRegion(String regionName, Properties props,
                                                          CacheDataDescription metadata) throws CacheException {
        return new HibernateNaturalIdRegion(this,
            regionName,
            accessStgyFactory.node(),
            accessStgyFactory.regionCache(regionName),
            metadata);
    }

    /** {@inheritDoc} */
    @Override public CollectionRegion buildCollectionRegion(String regionName, Properties props,
                                                            CacheDataDescription metadata) throws CacheException {
        return new HibernateCollectionRegion(this,
            regionName,
            accessStgyFactory.node(),
            accessStgyFactory.regionCache(regionName),
            metadata);
    }

    /** {@inheritDoc} */
    @Override public QueryResultsRegion buildQueryResultsRegion(String regionName, Properties props)
        throws CacheException {
        return new HibernateQueryResultsRegion(this,
            regionName,
            accessStgyFactory.node(),
            accessStgyFactory.regionCache(regionName));
    }

    /** {@inheritDoc} */
    @Override public TimestampsRegion buildTimestampsRegion(String regionName, Properties props) throws CacheException {
        return new HibernateTimestampsRegion(this,
            regionName,
            accessStgyFactory.node(),
            accessStgyFactory.regionCache(regionName));
    }
}