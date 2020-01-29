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

package org.apache.ignite.internal.processors.query.calcite.serialize.relation;

import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.ExpToRexTranslator;

/**
 * Provides context to complete RelGraph to RelNode tree conversion.
 */
public interface ConversionContext extends RelOptTable.ToRelContext {
    /**
     * @return Type factory.
     */
    RelDataTypeFactory getTypeFactory();

    /**
     * @return Schema.
     */
    RelOptSchema getSchema();

    /**
     * @return Planner context.
     */
    PlanningContext getContext();

    /**
     * @return Expression translator.
     */
    ExpToRexTranslator getExpressionTranslator();
}
