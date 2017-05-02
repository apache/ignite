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

import java.util.Collection;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Cache change batch.
 */
public class DynamicCacheChangeBatch implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Discovery custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** Change requests. */
    @GridToStringInclude
    private Collection<DynamicCacheChangeRequest> reqs;

    /** Cache updates to be executed on exchange. */
    private transient ExchangeActions exchangeActions;

    /**
     * @param reqs Requests.
     */
    public DynamicCacheChangeBatch(Collection<DynamicCacheChangeRequest> reqs) {
        assert !F.isEmpty(reqs) : reqs;

        this.reqs = reqs;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /**
     * @return Collection of change requests.
     */
    public Collection<DynamicCacheChangeRequest> requests() {
        return reqs;
    }

    /**
     * @return {@code True} if request should trigger partition exchange.
     */
    public boolean exchangeNeeded() {
        return exchangeActions != null;
    }

    /**
     * @return Cache updates to be executed on exchange.
     */
    ExchangeActions exchangeActions() {
        return exchangeActions;
    }

    /**
     * @param exchangeActions Cache updates to be executed on exchange.
     */
    void exchangeActions(ExchangeActions exchangeActions) {
        assert exchangeActions != null && !exchangeActions.empty() : exchangeActions;

        this.exchangeActions = exchangeActions;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheChangeBatch.class, this);
    }
}