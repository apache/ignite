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

package org.apache.ignite.internal.pagemem.size;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.pagemem.size.group.DataStructureSizePersistentGroup;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.INTERNAL;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PARTITION;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PK_INDEX;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PURE_DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.REUSE_LIST;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.doubleSizeUpdate;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.sizeAndParentUpdate;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.sizeWithTrackingPages;

public class DataStructureSizePartition implements DataStructureSizeContext<Void, DataStructureSizeContext> {
    private final DataStructureSizePersistentGroup groupLevel;

    private final String name;

    private final Map<String, DataStructureSize> structures = new ConcurrentLinkedHashMap<>();

    private final int pageSize;

    public DataStructureSizePartition(DataStructureSizePersistentGroup grpLevel, String name, int pageSize) {
        this.groupLevel = grpLevel;
        this.name = name;
        this.pageSize = pageSize;

        DataStructureSize pkIndexPages = sizeAndParentUpdate(name + "-" + PK_INDEX, groupLevel.sizeOf(PK_INDEX));

        DataStructureSize reuseListPages = sizeAndParentUpdate(name + "-" + REUSE_LIST, grpLevel.sizeOf(REUSE_LIST));

        DataStructureSize dataPages = sizeAndParentUpdate(name + "-" + DATA, grpLevel.sizeOf(DATA));

        DataStructureSize pureDataSize = sizeAndParentUpdate(name + "-" + PURE_DATA, grpLevel.sizeOf(PURE_DATA));

        DataStructureSize internalSize = sizeAndParentUpdate(name + "-" + INTERNAL, grpLevel.sizeOf(INTERNAL));

        DataStructureSize partitionSize = sizeWithTrackingPages(name + "-" + PARTITION, internalSize, pageSize);

        DataStructureSize partitionPages = doubleSizeUpdate(partitionSize, grpLevel.sizeOf(PARTITION));

        structures.put(pkIndexPages.name(), pkIndexPages);
        structures.put(reuseListPages.name(), reuseListPages);
        structures.put(dataPages.name(), dataPages);
        structures.put(pureDataSize.name(), pureDataSize);
        structures.put(internalSize.name(), internalSize);
        structures.put(partitionPages.name(), partitionPages);

    }

    @Override public DataStructureSizeContext parent() {
        return groupLevel;
    }

    @Override public Collection<DataStructureSizeContext> childes() {
        throw new UnsupportedOperationException();
    }

    @Override public DataStructureSizeContext createChild(Void context) {
        throw new UnsupportedOperationException();
    }

    @Override public Collection<DataStructureSize> structures() {
        return structures.values();
    }

    @Override public DataStructureSize sizeOf(String struct) {
        return structures.get(name + "-" + struct);
    }

    @Override public String name() {
        return name;
    }
}
