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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.UUID;

/**
 * Future for dynamic index create/drop returned to the client..
 */
public class QueryIndexClientFuture extends GridFutureAdapter<Object> {
    /** Operation ID. */
    private final UUID opId;

    /** Key. */
    private final QueryIndexKey key;

    /**
     * Constructor.
     *
     * @param opId Operation ID.
     * @param key Key.
     */
    public QueryIndexClientFuture(UUID opId, QueryIndexKey key) {
        this.opId = opId;
        this.key = key;
    }

    /**
     * @return Operation ID.
     */
    public UUID operationId() {
        return opId;
    }

    /**
     * @return Index key.
     */
    public QueryIndexKey key() {
        return key;
    }

    /**
     * Handle cache stop.
     */
    public void onCacheStopped() {
        onDone(new IgniteException("Operation failed because cache was stopped."));
    }

    /**
     * Handle type undeploy.
     */
    public void onTypeUnregistered() {
        onDone(new IgniteException("Operation failed because type was undeployed."));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryIndexClientFuture.class, this);
    }
}
