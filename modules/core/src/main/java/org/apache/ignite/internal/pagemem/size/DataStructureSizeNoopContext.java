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
import java.util.Collections;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;

public class DataStructureSizeNoopContext implements DataStructureSizeContext {
    @Override public DataStructureSizeContext parent() {
        return this;
    }

    @Override public Collection<DataStructureSizeContext> childes() {
        return Collections.emptyList();
    }

    @Override public DataStructureSizeContext createChild(Object context) {
        return this;
    }

    @Override public Collection<DataStructureSize> structures() {
        return Collections.emptyList();
    }

    @Override public DataStructureSize sizeOf(String structure) {
        return new DataStructureSizeAdapter() { };
    }

    @Override public String name() {
        return "NOOP";
    }
}
