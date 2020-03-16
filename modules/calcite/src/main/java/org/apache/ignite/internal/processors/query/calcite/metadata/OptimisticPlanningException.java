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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import org.apache.calcite.rel.RelNode;

/**
 *
 */
public class OptimisticPlanningException extends RuntimeException{
    /** */
    private final RelNode node;

    /**
     *
     * @param message Message.
     * @param node Node of a query plan, where the exception was thrown.
     * @param cause Cause.
     */
    public OptimisticPlanningException(String message, RelNode node, Throwable cause) {
        super(message, cause);
        this.node = node;
    }

    /**
     * @return Node of a query plan, where the exception was thrown.
     */
    public RelNode node() {
        return node;
    }
}
