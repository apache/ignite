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

package org.apache.ignite.spi.monitoring.sql;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.monitoring.MonitoringGroup;
import org.apache.ignite.internal.processors.monitoring.lists.ComputeTaskMonitoringInfo;
import org.apache.ignite.internal.processors.monitoring.lists.MonitoringList;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlAbstractLocalSystemView;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.services.ServiceMonitoringInfo;
import org.apache.ignite.spi.monitoring.sql.lists.ComputeTaskInfoRowListHelper;
import org.apache.ignite.spi.monitoring.sql.lists.ListRowHelper;
import org.apache.ignite.spi.monitoring.sql.lists.ServiceInfoRowListHelper;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 *
 */
public class ListSystemView<Id, ListRow> extends SqlAbstractLocalSystemView {
    /** */
    private MonitoringGroup grp;

    /** */
    private MonitoringList<Id, ListRow> list;

    /** */
    private ListRowHelper<Id, ListRow> rowHelper;

    /** */
    private static Map<Class, ListRowHelper> ROW_HELPERS = new HashMap<>();

    static {
        ROW_HELPERS.put(ComputeTaskMonitoringInfo.class, new ComputeTaskInfoRowListHelper());
        ROW_HELPERS.put(ServiceMonitoringInfo.class, new ServiceInfoRowListHelper());
    }

    /** */
    public ListSystemView(T2<MonitoringGroup, MonitoringList<Id, ListRow>> list, GridKernalContext ctx) {
        super("LIST_" + list.get1().name().toUpperCase(),
            "List of " + list.get1().name(),
            ctx,
            ROW_HELPERS.get(list.get2().rowClass()).columns());

        System.out.println();

        this.grp = list.get1();
        this.list = list.get2();
        this.rowHelper = ROW_HELPERS.get(list.get2().rowClass());
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        //TODO: support indexed id column

        return F.iterator(list.iterator(), row -> createRow(ses, rowHelper.row(row)), true);
    }
}
