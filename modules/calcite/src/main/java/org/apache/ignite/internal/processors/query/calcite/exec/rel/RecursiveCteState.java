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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Query-local current and next deltas of a recursive CTE. */
public class RecursiveCteState<Row> {
    /** Rows visible to the recursive table scan. */
    private List<Row> current = Collections.emptyList();

    /** Rows produced by the active seed or recursive term. */
    private List<Row> next;

    /** Starts collecting the next delta. */
    public void beginWrite() {
        next = new ArrayList<>();
    }

    /** Adds a row to the next delta. */
    public void add(Row row) {
        assert next != null;

        next.add(row);
    }

    /** Makes the collected delta visible to recursive scans. */
    public void commit() {
        assert next != null;

        current = next;
        next = null;
    }

    /** Current delta. */
    public Iterable<Row> current() {
        return () -> current.iterator();
    }

    /** Clears all query-local rows. */
    public void clear() {
        current = Collections.emptyList();
        next = null;
    }
}
