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

package org.apache.ignite.internal.processors.query.calcite.exec.partition;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** */
public class PartitionOperandNode implements PartitionNode {
    /** */
    private final Operand op;

    /** */
    private final List<PartitionNode> operands;

    /** */
    private PartitionOperandNode(Operand op, List<PartitionNode> operands) {
        this.op = op;
        this.operands = Collections.unmodifiableList(operands);
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(PartitionPruningContext ctx) {
        Set<Integer> allParts = null;

        if (op == Operand.AND) {
            for (PartitionNode operand : operands) {
                Collection<Integer> parts = operand.apply(ctx);

                if (parts == null)
                    continue;

                if (allParts == null)
                    allParts = new HashSet<>(parts);
                else
                    allParts.retainAll(parts);
            }
        }
        else {
            for (PartitionNode operand: operands) {
                Collection<Integer> parts = operand.apply(ctx);

                if (parts == null)
                    break;

                if (allParts == null)
                    allParts = new HashSet<>(parts);
                else
                    allParts.addAll(parts);
            }
        }

        return allParts != null ? Collections.unmodifiableCollection(allParts) : null;
    }

    /** {@inheritDoc} */
    @Override public PartitionNode optimize() {
        switch (op) {
            case OR:
                if (operands.stream().anyMatch(n -> n == PartitionAllNode.INSTANCE))
                    return PartitionAllNode.INSTANCE;

                break;
            case AND:
                if (operands.stream().anyMatch(n -> n == PartitionNoneNode.INSTANCE))
                    return PartitionNoneNode.INSTANCE;
        }

        return this;
    }

    /** */
    public static PartitionOperandNode createAndOperandNode(List<PartitionNode> operands) {
        return new PartitionOperandNode(Operand.AND, operands);
    }

    /** */
    public static PartitionOperandNode createOrOperandNode(List<PartitionNode> operands) {
        return new PartitionOperandNode(Operand.OR, operands);
    }

    /** */
    private enum Operand {
        /** */
        AND,

        /** */
        OR,
    }
}
