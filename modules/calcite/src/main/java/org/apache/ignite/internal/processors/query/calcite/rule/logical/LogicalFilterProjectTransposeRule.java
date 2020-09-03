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

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;

/**
 *
 */
public class LogicalFilterProjectTransposeRule extends FilterProjectTransposeRule {
    /** */
    public static final RelOptRule INSTANCE = new LogicalFilterProjectTransposeRule();

    /** */
    public LogicalFilterProjectTransposeRule() {
        super(LogicalFilter.class, filter -> true, LogicalProject.class, project -> true, true, true,
            RelFactories.LOGICAL_BUILDER);
    }
}
