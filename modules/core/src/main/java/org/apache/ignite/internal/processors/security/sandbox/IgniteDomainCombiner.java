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

package org.apache.ignite.internal.processors.security.sandbox;

import java.lang.ref.WeakReference;
import java.security.DomainCombiner;
import java.security.PermissionCollection;
import java.security.ProtectionDomain;
import java.util.WeakHashMap;

/**
 * A {@code IgniteSubjectDomainCombainer} updates ProtectionDomains with passed {@code Permissions}.
 */
public class IgniteDomainCombiner implements DomainCombiner {
    /** */
    private final WeakKeyValueMap<ProtectionDomain, ProtectionDomain> cachedPDs = new WeakKeyValueMap<>();

    /** */
    private final PermissionCollection perms;

    /** */
    public IgniteDomainCombiner(PermissionCollection perms) {
        this.perms = perms;
    }

    /** {@inheritDoc} */
    @Override public ProtectionDomain[] combine(ProtectionDomain[] currDomains, ProtectionDomain[] assignedDomains) {
        if (currDomains == null || currDomains.length == 0)
            return assignedDomains;

        if (currDomains == null && assignedDomains == null)
            return null;

        ProtectionDomain[] newDomains = null;

        if (currDomains != null && currDomains.length > 0) {
            newDomains = new ProtectionDomain[1];

            synchronized (cachedPDs) {
                ProtectionDomain pd = currDomains[0];

                ProtectionDomain subjectPd = cachedPDs.getValue(pd);

                if (subjectPd == null) {
                    subjectPd = new ProtectionDomain(pd.getCodeSource(), perms);

                    cachedPDs.putValue(pd, subjectPd);
                }

                newDomains[0] = subjectPd;
            }
        }

        return newDomains == null || newDomains.length == 0 ? null : newDomains;
    }

    /** */
    private static class WeakKeyValueMap<K, V> extends WeakHashMap<K, WeakReference<V>> {
        /** */
        public V getValue(K key) {
            WeakReference<V> wr = get(key);

            return wr != null ? wr.get() : null;
        }

        /** */
        public V putValue(K key, V val) {
            WeakReference<V> wr = put(key, new WeakReference<>(val));

            return wr != null ? wr.get() : null;
        }
    }
}
