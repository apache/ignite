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

package org.apache.ignite.internal.processors.query.h2.ddl;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * DDL schema version. Defines certain schema state and allows to filter unwanted stale events.
 */
public class DdlSchemaVersion implements Comparable<DdlSchemaVersion> {
    /** Topology version. */
    private final long topVer;

    /** Counter. */
    private final long ctr;

    /**
     * Constructor.
     *
     * @param topVer Topology version.
     * @param ctr Counter.
     */
    public DdlSchemaVersion(long topVer, long ctr) {
        this.topVer = topVer;
        this.ctr = ctr;
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Counter.
     */
    public long counter() {
        return ctr;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (int)(topVer ^ (topVer >>> 32)) + (int)(ctr ^ (ctr >>> 32));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj != null && obj instanceof DdlSchemaVersion) {
            DdlSchemaVersion other = (DdlSchemaVersion)obj;

            return ctr == other.ctr && topVer == other.topVer;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull DdlSchemaVersion other) {
        long delta = topVer - other.topVer;

        if (delta == 0)
            delta = ctr - other.ctr;

        return delta > 0 ? 1 : delta < 0 ? -1 : 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DdlSchemaVersion.class, this);
    }
}
