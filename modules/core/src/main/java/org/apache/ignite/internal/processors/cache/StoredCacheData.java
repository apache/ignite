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

import java.io.Serializable;
import java.util.Collection;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

/**
 * Cache data to write to and read from {@link IgnitePageStoreManager}. In a nutshell, contains (most importantly)
 * {@link CacheConfiguration} and additional information about cache which is not a part of configuration.
 * This class is {@link Serializable} and is intended to be read-written with {@link JdkMarshaller}
 * in order to be serialization wise agnostic to further additions or removals of fields.
 */
public class StoredCacheData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache configuration. */
    @GridToStringInclude
    private CacheConfiguration<?, ?> ccfg;

    /** Query entities. */
    @GridToStringInclude
    private Collection<QueryEntity> qryEntities;

    /** SQL flag - {@code true} if cache was created with {@code CREATE TABLE}. */
    private boolean sql;

    /**
     * Constructor.
     *
     * @param ccfg Cache configuration.
     */
    public StoredCacheData(CacheConfiguration<?, ?> ccfg) {
        A.notNull(ccfg, "ccfg");

        this.ccfg = ccfg;
        this.qryEntities = ccfg.getQueryEntities();
    }

    /**
     * @param ccfg Cache configuration.
     */
    public void config(CacheConfiguration<?, ?> ccfg) {
        this.ccfg = ccfg;
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration<?, ?> config() {
        return ccfg;
    }

    /**
     * @return Query entities.
     */
    public Collection<QueryEntity> queryEntities() {
        return qryEntities;
    }

    /**
     * @param qryEntities Query entities.
     */
    public void queryEntities(Collection<QueryEntity> qryEntities) {
        this.qryEntities = qryEntities;
    }

    /**
     * @return SQL flag - {@code true} if cache was created with {@code CREATE TABLE}.
     */
    public boolean sql() {
        return sql;
    }

    /**
     * @param sql SQL flag - {@code true} if cache was created with {@code CREATE TABLE}.
     */
    public StoredCacheData sql(boolean sql) {
        this.sql = sql;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StoredCacheData.class, this);
    }
}
