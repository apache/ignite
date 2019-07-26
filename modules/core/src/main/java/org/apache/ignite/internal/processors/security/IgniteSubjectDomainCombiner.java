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

package org.apache.ignite.internal.processors.security;

import java.lang.ref.WeakReference;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.util.WeakHashMap;
import javax.security.auth.Subject;
import javax.security.auth.SubjectDomainCombiner;

/**
 * A {@code IgniteSubjectDomainCombainer} updates ProtectionDomains with passed {@code Permissions}.
 */
public class IgniteSubjectDomainCombiner extends SubjectDomainCombiner {
    /** . */
    private final WeakKeyValueMap<ProtectionDomain, ProtectionDomain> cachedPDs = new WeakKeyValueMap<>();

    /** . */
    private final Permissions perms;

    /** . */
    public IgniteSubjectDomainCombiner(Subject subject, Permissions perms) {
        super(subject);

        this.perms = perms;
    }

    /** {@inheritDoc} */
    @Override public ProtectionDomain[] combine(ProtectionDomain[] currDomains, ProtectionDomain[] assignedDomains) {
        if (currDomains == null || currDomains.length == 0)
            return assignedDomains;

        currDomains = optimize(currDomains);

        if (currDomains == null && assignedDomains == null)
            return null;

        int cLen = currDomains == null ? 0 : currDomains.length;

        ProtectionDomain[] newDomains = new ProtectionDomain[cLen];

        synchronized (cachedPDs) {
            ProtectionDomain subjectPd;

            for (int i = 0; i < cLen; i++) {
                ProtectionDomain pd = currDomains[i];

                subjectPd = cachedPDs.getValue(pd);

                if (subjectPd == null) {
                    subjectPd = new ProtectionDomain(pd.getCodeSource(), perms);

                    cachedPDs.putValue(pd, subjectPd);
                }

                newDomains[i] = subjectPd;
            }
        }

        return newDomains == null || newDomains.length == 0 ? null : newDomains;
    }

    /** . */
    private static ProtectionDomain[] optimize(ProtectionDomain[] domains) {
        if (domains == null || domains.length == 0)
            return null;

        ProtectionDomain[] optimized = new ProtectionDomain[domains.length];

        ProtectionDomain pd;

        int num = 0;

        for (int i = 0; i < domains.length; i++) {
            if ((pd = domains[i]) != null) {
                boolean found = false;

                for (int j = 0; j < num && !found; j++)
                    found = (optimized[j] == pd);

                if (!found)
                    optimized[num++] = pd;
            }
        }

        // resize the array if necessary
        if (num > 0 && num < domains.length) {
            ProtectionDomain[] downSize = new ProtectionDomain[num];

            System.arraycopy(optimized, 0, downSize, 0, downSize.length);

            optimized = downSize;
        }

        return num == 0 || optimized.length == 0 ? null : optimized;
    }

    /** . */
    private static class WeakKeyValueMap<K, V> extends WeakHashMap<K, WeakReference<V>> {
        /** . */
        public V getValue(K key) {
            WeakReference<V> wr = get(key);

            return wr != null ? wr.get() : null;
        }

        /** . */
        public V putValue(K key, V val) {
            WeakReference<V> wr = put(key, new WeakReference<>(val));

            return wr != null ? wr.get() : null;
        }
    }
}
