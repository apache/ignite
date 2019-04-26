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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition reservation key.
 */
public class PartitionReservationKey {
    /** Cache name. */
    private final String cacheName;

    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     * @param topVer Topology version.
     */
    public PartitionReservationKey(String cacheName, AffinityTopologyVersion topVer) {
        this.cacheName = cacheName;
        this.topVer = topVer;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Topology version of reservation.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        PartitionReservationKey other = (PartitionReservationKey)o;

        return F.eq(cacheName, other.cacheName) && F.eq(topVer, other.topVer);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = cacheName != null ? cacheName.hashCode() : 0;

        res = 31 * res + (topVer != null ? topVer.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionReservationKey.class, this);
    }
}
