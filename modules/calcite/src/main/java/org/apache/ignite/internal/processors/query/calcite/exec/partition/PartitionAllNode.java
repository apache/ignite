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

/** */
public class PartitionAllNode implements PartitionNode {
    /** Exclude this node from partition calculation if other nodes present in {@link PartitionParameterNode }. */
    public static final PartitionAllNode IGNORE = new PartitionAllNode(true);

    /** */
    public static final PartitionAllNode INSTANCE = new PartitionAllNode(false);

    /** */
    private final boolean replicated;

    /** */
    private PartitionAllNode(boolean replicated) {
        this.replicated = replicated;
    }

    /** */
    public boolean isReplicated() {
        return replicated;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(PartitionPruningContext ctx) {
        return null;
    }
}
