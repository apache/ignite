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

import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;

/**
 * Regular query or DML
 */
public interface MultiStepPlan extends QueryPlan {
    /**
     * @return Fields metadata.
     */
    FieldsMetadata fieldsMetadata();

    /**
     * @return Parameters metadata;
     */
    FieldsMetadata paramsMetadata();

    /**
     * Inits query fragments.
     *
     * @param ctx Planner context.
     */
    ExecutionPlan init(MappingService mappingService, AffinityService affSvc, MappingQueryContext ctx);

    /**
     * @return Text representation of query plan
     */
    String textPlan();
}
