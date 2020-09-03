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

package org.apache.ignite.internal.processors.query.calcite.trait;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.ToIntFunction;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public final class Partitioned<Row> implements Destination<Row> {
    /** */
    private final List<List<UUID>> assignments;

    /** */
    private final ToIntFunction<Row> partFun;

    /** */
    public Partitioned(List<List<UUID>> assignments, ToIntFunction<Row> partFun) {
        this.assignments = assignments;
        this.partFun = partFun;
    }

    /** {@inheritDoc} */
    @Override public List<UUID> targets(Row row) {
        return assignments.get(partFun.applyAsInt(row));
    }

    /** {@inheritDoc} */
    @Override public List<UUID> targets() {
        Set<UUID> targets = U.newHashSet(assignments.size());
        for (List<UUID> assignment : assignments) {
            if (!F.isEmpty(assignment))
                targets.addAll(assignment);
        }

        return new ArrayList<>(targets);
    }
}
