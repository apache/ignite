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

package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CustomMessageWrapper implements DiscoverySpiCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final DiscoveryCustomMessage delegate;

    /**
     * @param delegate Delegate.
     */
    public CustomMessageWrapper(DiscoveryCustomMessage delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoverySpiCustomMessage ackMessage() {
        DiscoveryCustomMessage res = delegate.ackMessage();

        return res == null ? null : new CustomMessageWrapper(res);
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return delegate.isMutable();
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return delegate.stopProcess();
    }

    /**
     * @return Delegate.
     */
    public DiscoveryCustomMessage delegate() {
        return delegate;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return delegate.toString();
    }
}