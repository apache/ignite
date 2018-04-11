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

import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheObjectsReleaseFuture<T, R> extends GridCompoundFuture<T, R> {
    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private String type;

    /**
     * @param type Wait object type.
     * @param topVer Topology version to wait for.
     */
    public CacheObjectsReleaseFuture(String type, AffinityTopologyVersion topVer) {
        this.type = type;
        this.topVer = topVer;
    }

    /**
     * @param type Wait object type.
     * @param topVer Topology version to wait for.
     * @param rdc Reducer object.
     */
    public CacheObjectsReleaseFuture(String type, AffinityTopologyVersion topVer, @Nullable IgniteReducer<T, R> rdc) {
        super(rdc);

        this.topVer = topVer;
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return type + "ReleaseFuture [topVer=" + topVer + ", futures=" + futures() + "]";
    }
}
