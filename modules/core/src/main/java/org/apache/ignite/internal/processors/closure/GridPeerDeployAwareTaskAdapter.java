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

package org.apache.ignite.internal.processors.closure;

import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Peer deployment aware task adapter.
 */
public abstract class GridPeerDeployAwareTaskAdapter<T, R> extends ComputeTaskAdapter<T, R>
    implements GridPeerDeployAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Peer deploy aware class. */
    private transient GridPeerDeployAware pda;

    /**
     * Constructor that receives deployment information for task.
     *
     * @param pda Deployment information.
     */
    protected GridPeerDeployAwareTaskAdapter(@Nullable GridPeerDeployAware pda) {
        this.pda = pda;
    }

    /** {@inheritDoc} */
    @Override public Class<?> deployClass() {
        if (pda == null)
            pda = U.detectPeerDeployAware(this);

        return pda.deployClass();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader classLoader() {
        if (pda == null)
            pda = U.detectPeerDeployAware(this);

        return pda.classLoader();
    }
}