/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.service;

import java.io.Serializable;
import java.util.ArrayList;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Initial data of {@link IgniteServiceProcessor} to send in cluster on joining node.
 */
public class ServiceProcessorJoinNodeDiscoveryData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Static services configurations info. */
    public final ArrayList<ServiceInfo> staticServicesInfo;

    /**
     * @param staticServicesInfo Static services configurations info.
     */
    public ServiceProcessorJoinNodeDiscoveryData(@NotNull ArrayList<ServiceInfo> staticServicesInfo) {
        this.staticServicesInfo = staticServicesInfo;
    }

    /**
     * @return Static services configurations info.
     */
    public ArrayList<ServiceInfo> services() {
        return staticServicesInfo;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceProcessorJoinNodeDiscoveryData.class, this);
    }
}