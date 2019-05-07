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

package org.apache.ignite.internal.processors.monitoring.lists;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.internal.processors.monitoring.MonitoringGroup;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class MonitoringListImpl<Id, Row> implements MonitoringList<Id, Row> {
    private String name;

    private MonitoringGroup group;

    private Map<Id, ListRow<Id, Row>> rows = new HashMap<>();

    private Class<Row> rowClass;

    public MonitoringListImpl(MonitoringGroup group, String name, Class<Row> rowClass) {
        this.name = name;
        this.group = group;
        this.rowClass = rowClass;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void add(Id id, String sessionId, Row data) {
        ListRow<Id, Row> old = rows.putIfAbsent(id, new ListRowImpl<>(id, sessionId, data));

        assert old == null;
    }

    /** {@inheritDoc} */
    @Override public void add(Id id, String sessionId, String name, Row data) {
        ListRow<Id, Row> old = rows.putIfAbsent(id, new NamedListRowImpl<>(id, sessionId, name, data));

        assert old == null;
    }

    /** {@inheritDoc} */
    @Override public void update(Id id, Row data) {
        if (!rows.containsKey(id))
            return; //TODO: should we throw an exception here?

        rows.get(id).setData(data);
    }

    /** {@inheritDoc} */
    @Override public void remove(Id id) {
        rows.remove(id);
    }

    /** {@inheritDoc} */
    @Override public Class<Row> rowClass() {
        return rowClass;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<ListRow<Id, Row>> iterator() {
        return rows.values().iterator();
    }

    public Collection<ListRow<Id, Row>> getRows() {
        return rows.values();
    }
}
