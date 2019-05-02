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

package org.apache.ignite.spi.monitoring.jmx.lists;

import java.util.Map;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularType;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.monitoring.lists.ListRow;

/**
 *
 */
public abstract class ListRowMBeanHelper<Id, Row> {
    private TabularType listType;

    private CompositeType rowType;

    protected abstract String[] fields();

    protected abstract OpenType[] types();

    protected abstract Class<Row> rowClass();

    protected abstract Map<String, Object> values(ListRow<Id, Row> row);

    protected String key() {
        return "id";
    }

    public CompositeData row(ListRow<Id, Row> row) {
        try {
            return new CompositeDataSupport(rowType(), values(row));
        }
        catch (OpenDataException e) {
            throw new IgniteException(e);
        }
    }

    public TabularType listType() {
        if (listType != null)
            return listType;

        try {
            return listType = new TabularType(
                rowClass().getName(),
                "List of " + rowClass().getName(),
                rowType(),
                new String[] {key()}
            );
        }
        catch (OpenDataException e) {
            throw new IgniteException(e);
        }
    }

    public CompositeType rowType() {
        if (rowType != null)
            return rowType;

        try {
            return rowType = new CompositeType(rowClass().getName(),
                rowClass().getName(),
                fields(),
                fields(),
                types());
        }
        catch (OpenDataException e) {
            throw new IgniteException(e);
        }
    }
}
