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
import org.apache.ignite.internal.processors.monitoring.lists.ListRow;
import org.apache.ignite.services.ServiceMonitoringInfo;

/**
 *
 */
public class ServiceInfoRowListHelper extends ListRowMBeanHelper<UUID, ServiceMonitoringInfo> {

    @Override protected String[] fields() {
        return new String[] {
            "id",
            "name",
            "totalCnt",
            "maxPerNodeCnt",
            "cacheName"
        };
    }

    @Override protected OpenType[] types() {
        return new OpenType[] {
            SimpleType.STRING,
            SimpleType.STRING,
            SimpleType.INTEGER,
            SimpleType.INTEGER,
            SimpleType.STRING
        };
    }

    @Override protected Class<ServiceMonitoringInfo> rowClass() {
        return ServiceMonitoringInfo.class;
    }

    @Override protected Map<String, Object> values(ListRow<UUID, ServiceMonitoringInfo> row) {
        Map<String, Object> vals = new HashMap<>();

        ServiceMonitoringInfo data = row.getData();

        vals.put("id", row.getId().toString());
        vals.put("name", data.getName());
        vals.put("totalCnt", data.getTotalCnt());
        vals.put("maxPerNodeCnt", data.getMaxPerNodeCnt());
        vals.put("cacheName", data.getCacheName());

        return vals;
    }
}
