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

package org.apache.ignite.tests.p2p;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/**
 * Test key with an {@link AffinityKeyMapped} field. Available through an external class loader only.
 */
public class CacheDeploymentAffinityMappedKey {
    /** Affinity key. */
    @AffinityKeyMapped
    private int aff = 1;

    /** Key id. */
    private int id = 2;

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        CacheDeploymentAffinityMappedKey that = (CacheDeploymentAffinityMappedKey)o;

        return aff == that.aff && id == that.id;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * aff + id;
    }
}
