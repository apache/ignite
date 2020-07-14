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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.List;

import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

/**
 * Represents a node of execution tree.
 *
 * <p/><b>Note</b>: except several cases (like consumer node and mailboxes), {@link Node#request(int)}, {@link Node#cancel()},
 * {@link Downstream#push(Object)} and {@link Downstream#end()} methods should be used from one single thread.
 */
public interface Node<Row> {
    /**
     * Returns runtime context allowing access to the tables in a database.
     *
     * @return Execution context.
     */
    ExecutionContext<Row> context();

    /**
     * Registers node sources.
     *
     * @param sources Sources collection.
     */
    void register(List<Node<Row>> sources);

    /**
     * Registers downstream.
     *
     * @param downstream Downstream.
     */
    void onRegister(Downstream<Row> downstream);

    /**
     * Requests next bunch of rows.
     */
    void request(int rowsCnt);

    /**
     * Cancels execution.
     */
    void cancel();
}
