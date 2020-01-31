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

import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;

/**
 *
 */
public interface RelSource {
    /**
     * @return ID of a fragment, where data comes from.
     */
    long fragmentId();

    /**
     * Returns source mapping. The mapping contains nodes where data comes from.
     * It's used to determine that all sources sent all related data.
     *
     * @return Source mapping.
     */
    NodesMapping mapping();

    /**
     * Binds a source to target and starts source data location calculation.
     * After this method call the source knows where to send data and the target knows where to expect data from.
     * @param target Target.
     * @param mappingService
     * @param ctx Context.
     * @param mq Metadata query instance.
     */
    default void bindToTarget(RelTarget target, MappingService mappingService, PlanningContext ctx, RelMetadataQuery mq) {
        // No-op
    }
}
