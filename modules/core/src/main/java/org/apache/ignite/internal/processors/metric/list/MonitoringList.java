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

package org.apache.ignite.internal.processors.metric.list;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class MonitoringList<Id, R extends MonitoringRow<Id>> implements Iterable<R> {
    /** Name of the list. */
    private final String name;

    /** Data of the list. */
    private final ConcurrentHashMap<Id, R> data = new ConcurrentHashMap<>();

    /**
     * @param name Name of the list.
     */
    public MonitoringList(String name) {
        this.name = name;
    }

    /**
     * @param id Id of the row.
     * @param row Row.
     */
    public void add(Id id, R row) {
        MonitoringRow<Id> old = data.put(id, row);

        assert old == null;
    }

    /**
     * @param id Id of the row.
     * @return Removed row.
     */
    public MonitoringRow<Id> remove(Id id) {
        return data.remove(id);
    }

    /**
     * @return List name.
     */
    public String name() {
        return name;
    }

    @NotNull @Override public Iterator<R> iterator() {
        return data.values().iterator();
    }
}
