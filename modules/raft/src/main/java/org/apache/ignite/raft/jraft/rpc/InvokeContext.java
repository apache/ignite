/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.rpc;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * RPC invoke context.
 */
public class InvokeContext {
    private final ConcurrentMap<String, Object> ctx = new ConcurrentHashMap<>();

    public Object put(final String key, final Object value) {
        return this.ctx.put(key, value);
    }

    public <T> T get(final String key) {
        return (T) this.ctx.get(key);
    }

    public <T> T getOrDefault(final String key, final T defaultValue) {
        return (T) this.ctx.getOrDefault(key, defaultValue);
    }

    public void clear() {
        this.ctx.clear();
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        return this.ctx.entrySet();
    }
}
