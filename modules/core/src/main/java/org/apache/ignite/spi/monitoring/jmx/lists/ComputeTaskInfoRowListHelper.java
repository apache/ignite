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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import org.apache.ignite.internal.processors.monitoring.lists.ComputeTaskMonitoringInfo;
import org.apache.ignite.internal.processors.monitoring.lists.ListRow;

/**
 *
 */
public class ComputeTaskInfoRowListHelper extends ListRowMBeanHelper<UUID, ComputeTaskMonitoringInfo> {
    @Override protected String[] fields() {
        return new String[] {
            "id",
            "taskClasName",
            "startTime",
            "timeout",
            "execName"
        };
    }

    @Override protected OpenType[] types() {
        return new OpenType[] {
            SimpleType.STRING,
            SimpleType.STRING,
            SimpleType.LONG,
            SimpleType.LONG,
            SimpleType.STRING
        };
    }

    @Override protected Class<ComputeTaskMonitoringInfo> rowClass() {
        return ComputeTaskMonitoringInfo.class;
    }

    @Override protected Map<String, Object> values(ListRow<UUID, ComputeTaskMonitoringInfo> row) {
        Map<String, Object> vals = new HashMap<>();

        vals.put("id", row.getId().toString());
        vals.put("taskClasName", row.getData().getTaskClasName());
        vals.put("startTime", row.getData().getStartTime());
        vals.put("timeout", row.getData().getTimeout());
        vals.put("execName", row.getData().getExecName());

        return vals;
    }
}
