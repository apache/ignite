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

package org.apache.ignite.spi.monitoring.sql.lists;

import java.util.UUID;
import org.apache.ignite.internal.processors.monitoring.lists.ComputeTaskMonitoringInfo;
import org.apache.ignite.internal.processors.monitoring.lists.ListRow;
import org.h2.table.Column;
import org.h2.value.Value;

import static org.apache.ignite.internal.processors.query.h2.sys.view.SqlAbstractSystemView.newColumn;

/**
 *
 */
public class ComputeTaskInfoRowListHelper extends ListRowHelper<UUID, ComputeTaskMonitoringInfo> {
    /** {@inheritDoc} */
    @Override public Column[] columns() {
        return new Column[] {
            newColumn("id", Value.UUID),
            newColumn("taskClasName", Value.STRING),
            newColumn("startTime", Value.LONG),
            newColumn("timeout", Value.LONG),
            newColumn("execName", Value.STRING)
        };
    }

    /** {@inheritDoc} */
    @Override public Object[] row(ListRow<UUID, ComputeTaskMonitoringInfo> row) {
        return new Object[] {
            row.getId(),
            row.getData().getTaskClasName(),
            row.getData().getStartTime(),
            row.getData().getTimeout(),
            row.getData().getExecName()
        };
    }

    /** {@inheritDoc} */
    @Override protected Class<ComputeTaskMonitoringInfo> rowClass() {
        return ComputeTaskMonitoringInfo.class;
    }
}
