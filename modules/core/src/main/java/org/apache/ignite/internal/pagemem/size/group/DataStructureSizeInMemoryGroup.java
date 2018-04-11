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

package org.apache.ignite.internal.pagemem.size.group;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.pagemem.size.DataStructureSize;
import org.apache.ignite.internal.pagemem.size.DataStructureSizeContext;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;

import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PK_INDEX;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.simpleTracker;

public class DataStructureSizeInMemoryGroup implements DataStructureSizeContext<String, DataStructureSizeContext> {

    private final String name;

    private final DataStructureSizeContext parent;

    private final DataStructureSize pkIndex;

    public DataStructureSizeInMemoryGroup(DataStructureSizeContext parent, String name) {
        this.parent = parent;
        this.name = name;
        this.pkIndex = simpleTracker(name + "-" + PK_INDEX);
    }

    @Override public DataStructureSizeContext parent() {
        return parent;
    }

    @Override public Collection<DataStructureSizeContext> childes() {
        throw new UnsupportedOperationException();
    }

    @Override public DataStructureSizeContext createChild(String context) {
        return null;
    }

    @Override public Collection<DataStructureSize> structures() {
        return Collections.emptyList();
    }

    @Override public DataStructureSize sizeOf(String structure) {
        return new DataStructureSizeAdapter() {

            @Override public String name() {
                return name + "-" + structure;
            }
        };
    }

    @Override public String name() {
        return name;
    }
}
