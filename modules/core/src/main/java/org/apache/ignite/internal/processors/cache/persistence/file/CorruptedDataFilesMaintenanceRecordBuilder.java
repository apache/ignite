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

package org.apache.ignite.internal.processors.cache.persistence.file;

import org.apache.ignite.maintenance.MaintenanceRecordBuilder;

import java.util.Collection;
import java.util.UUID;

public class CorruptedDataFilesMaintenanceRecordBuilder implements MaintenanceRecordBuilder {
    /** */
    private static final String MNTC_RECORD_DESCR = "";

    /** */
    private final UUID typeId;

    /** */
    private final Collection<String> cacheGroupNames;

    public CorruptedDataFilesMaintenanceRecordBuilder(UUID typeId, Collection<String> cacheGroupNames) {
        this.typeId = typeId;
        this.cacheGroupNames = cacheGroupNames;
    }

    /** {@inheritDoc} */
    @Override public UUID maintenanceTypeId() {
        return typeId;
    }

    /** {@inheritDoc} */
    @Override public String maintenanceDescription() {
        return MNTC_RECORD_DESCR;
    }

    /** {@inheritDoc} */
    @Override public String getMaintenanceRecord() {
        return cacheGroupNames.toString();
    }
}
