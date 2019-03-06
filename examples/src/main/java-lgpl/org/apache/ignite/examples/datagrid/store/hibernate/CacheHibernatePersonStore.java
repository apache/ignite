/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.examples.datagrid.store.hibernate;

import java.util.List;
import java.util.UUID;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.hibernate.HibernateException;
import org.hibernate.Session;

/**
 * Example of {@link CacheStore} implementation that uses Hibernate
 * and deals with maps {@link UUID} to {@link Person}.
 */
public class CacheHibernatePersonStore extends CacheStoreAdapter<Long, Person> {
    /** Auto-injected store session. */
    @CacheStoreSessionResource
    private CacheStoreSession ses;

    /** {@inheritDoc} */
    @Override public Person load(Long key) {
        System.out.println(">>> Store load [key=" + key + ']');

        Session hibSes = ses.attachment();

        try {
            return (Person)hibSes.get(Person.class, key);
        }
        catch (HibernateException e) {
            throw new CacheLoaderException("Failed to load value from cache store [key=" + key + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(javax.cache.Cache.Entry<? extends Long, ? extends Person> entry) {
        Long key = entry.getKey();
        Person val = entry.getValue();

        System.out.println(">>> Store write [key=" + key + ", val=" + val + ']');

        Session hibSes = ses.attachment();

        try {
            hibSes.saveOrUpdate(val);
        }
        catch (HibernateException e) {
            throw new CacheWriterException("Failed to put value to cache store [key=" + key + ", val" + val + "]", e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"JpaQueryApiInspection"})
    @Override public void delete(Object key) {
        System.out.println(">>> Store delete [key=" + key + ']');

        Session hibSes = ses.attachment();

        try {
            hibSes.createQuery("delete " + Person.class.getSimpleName() + " where key = :key").
                setParameter("key", key).
                executeUpdate();
        }
        catch (HibernateException e) {
            throw new CacheWriterException("Failed to remove value from cache store [key=" + key + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<Long, Person> clo, Object... args) {
        if (args == null || args.length == 0 || args[0] == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.");

        final int entryCnt = (Integer)args[0];

        Session hibSes = ses.attachment();

        try {
            int cnt = 0;

            List list = hibSes.createCriteria(Person.class).
                setMaxResults(entryCnt).
                list();

            if (list != null) {
                for (Object obj : list) {
                    Person person = (Person)obj;

                    clo.apply(person.id, person);

                    cnt++;
                }
            }

            System.out.println(">>> Loaded " + cnt + " values into cache.");
        }
        catch (HibernateException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }
}
