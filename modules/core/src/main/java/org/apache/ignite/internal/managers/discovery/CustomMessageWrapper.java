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
    CustomMessageWrapper(DiscoveryCustomMessage delegate) {
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