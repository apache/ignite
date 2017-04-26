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

import org.apache.ignite.internal.util.typedef.F;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ExchangeActions {
    /** */
    private List<ActionData<DynamicCacheDescriptor>> cachesToStart;

    /** */
    private List<ActionData<DynamicCacheDescriptor>> clientCachesToStart;

    /** */
    private List<ActionData<String>> cachesToStop;

    /** */
    private List<ActionData<String>> cachesToClose;

    void addCacheToStart(DynamicCacheChangeRequest req, DynamicCacheDescriptor desc) {
        if (cachesToStart == null)
            cachesToStart = new ArrayList<>();

        cachesToStart.add(new ActionData<>(req, desc));
    }

    void addClientCacheToStart(DynamicCacheChangeRequest req, DynamicCacheDescriptor desc) {
        if (clientCachesToStart == null)
            clientCachesToStart = new ArrayList<>();

        clientCachesToStart.add(new ActionData<>(req, desc));
    }

    void addCacheToStop(DynamicCacheChangeRequest req) {
        if (cachesToStop == null)
            cachesToStop = new ArrayList<>();

        cachesToStop.add(new ActionData<>(req, req.cacheName()));
    }

    void addCacheToClose(DynamicCacheChangeRequest req) {
        if (cachesToClose == null)
            cachesToClose = new ArrayList<>();

        cachesToClose.add(new ActionData<>(req, req.cacheName()));
    }

    boolean empty() {
        return F.isEmpty(cachesToStart) &&
            F.isEmpty(clientCachesToStart) &&
            F.isEmpty(cachesToStop) &&
            F.isEmpty(cachesToClose);
    }

    void addFutureToComplete() {

    }

    static class ActionData<T> {
        DynamicCacheChangeRequest req;

        T data;

        public ActionData(DynamicCacheChangeRequest req, T data) {
            this.req = req;
            this.data = data;
        }
    }
}
