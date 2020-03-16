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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.io.Serializable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.PartitionService;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;

/**
 *
 */
public interface RelTarget extends Serializable {
    /**
     * @return Target fragment ID.
     */
    long fragmentId();

    /**
     * Returns target mapping. It's used in calculation a nodes list, where to send a particular row to.
     * See {@link DistributionFunction#destination(PartitionService, NodesMapping, ImmutableIntList)}
     *
     * @return Target mapping.
     */
    NodesMapping mapping();
}
