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

package org.apache.ignite.internal.managers.systemview;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.NotNull;

/**
 * System view implementation.
 * Data stored in the internal {@link ConcurrentMap}.
 * Owner of the list is responsible for the list fullfill and cleanup.
 */
public class SystemViewMap<K, R> extends AbstractSystemView<R> {
    /** List data */
    private final ConcurrentMap<K, R> data = new ConcurrentHashMap<>();

    /**
     * @param name Name.
     * @param desc Description.
     * @param rowCls Row class.
     * @param walker Walker.
     */
    public SystemViewMap(String name, String desc, Class<R> rowCls,
        SystemViewRowAttributeWalker<R> walker) {
        super(name, desc, rowCls, walker);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return data.size();
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<R> iterator() {
        return data.values().iterator();
    }

    public void add(K key, R row) {
        data.put(key, row);
    }

    public void remove(K key) {
        data.remove(key);
    }
}
