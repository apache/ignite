/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rex.RexCall;

/**
 * Implements a table-valued function call.
 */
public interface TableFunctionCallImplementor {
    /**
     * Implements a table-valued function call.
     *
     * @param translator Translator for the call.
     * @param inputEnumerable Table parameter of the call.
     * @param call Call that should be implemented.
     * @param inputPhysType Physical type of the table parameter.
     * @param outputPhysType Physical type of the call.
     * @return Expression that implements the call.
     */
    Expression implement(
        RexToLixTranslator translator,
        Expression inputEnumerable,
        RexCall call,
        PhysType inputPhysType,
        PhysType outputPhysType
    );
}
