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

package org.apache.ignite.cache.hibernate;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;

/**
 * Hibernate cache key wrapper.
 */
public class HibernateKeyWrapper implements Serializable {
    /** Key. */
    private final Object key;

    /** Entry. */
    private final String entry;

    /** */
    private final String tenantId;

    /**
     * @param key Key.
     * @param entry Entry.
     * @param tenantId Tenant ID.
     */
    HibernateKeyWrapper(Object key, String entry, String tenantId) {
        this.key = key;
        this.entry = entry;
        this.tenantId = tenantId;
    }

    /**
     * @return ID.
     */
    Object id() {
        return key;
    }

    /**
     * @param id ID.
     * @param persister Persister.
     * @param tenantIdentifier Tenant ID.
     * @return Cache key.
     * @see DefaultCacheKeysFactory#staticCreateCollectionKey(Object, CollectionPersister, SessionFactoryImplementor, String)
     */
    static Object staticCreateCollectionKey(Object id,
        CollectionPersister persister,
        String tenantIdentifier) {
        return new HibernateKeyWrapper(id, persister.getRole(), tenantIdentifier);
    }

    /**
     * @param id ID.
     * @param persister Persister.
     * @param tenantIdentifier Tenant ID.
     * @return Cache key.
     * @see DefaultCacheKeysFactory#staticCreateEntityKey(Object, EntityPersister, SessionFactoryImplementor, String)
     */
    public static Object staticCreateEntityKey(Object id, EntityPersister persister, String tenantIdentifier) {
        return new HibernateKeyWrapper(id, persister.getRootEntityName(), tenantIdentifier);
    }


    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass())
            return false;

        HibernateKeyWrapper that = (HibernateKeyWrapper) o;

        return (key != null ? key.equals(that.key) : that.key == null) &&
            (entry != null ? entry.equals(that.entry) : that.entry == null) &&
            (tenantId != null ? tenantId.equals(that.tenantId) : that.tenantId == null);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = key != null ? key.hashCode() : 0;
        res = 31 * res + (entry != null ? entry.hashCode() : 0);
        res = 31 * res + (tenantId != null ? tenantId.hashCode() : 0);
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HibernateKeyWrapper.class, this);
    }
}
