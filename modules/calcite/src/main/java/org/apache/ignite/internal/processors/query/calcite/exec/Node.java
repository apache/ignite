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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.List;

/**
 * Represents a node of execution tree.
 *
 * <p/><b>Note</b>: except several cases (like consumer node and mailboxes), {@link Node#request()}, {@link Node#cancel()},
 * {@link Node#reset()}, {@link Sink#push(Object)} and {@link Sink#end()} methods should be used from one single thread.
 */
public interface Node<T> {
    /**
     * Returns runtime context allowing access to the tables in a database.
     *
     * @return Execution context.
     */
    ExecutionContext context();

    /**
     * Requests a sink of the node. The sink is used to push data into the node by its children.
     *
     * @param idx Sink index.
     * @return Sink object.
     * @throws IndexOutOfBoundsException in case there is no Sink object associated with given index.
     */
    Sink<T> sink(int idx);

    /**
     * Registers target sink.
     *
     * @param sink Target sink.
     */
    void target(Sink<T> sink);

    /**
     * @return Registered target.
     */
    Sink<T> target();

    /**
     * @return Node inputs collection.
     */
    List<Node<T>> inputs();

    /**
     * @param idx Input index.
     * @return Node input.
     */
    default Node<T> input(int idx) {
        return inputs().get(idx);
    }

    /**
     * Signals that consumer is ready to consume data.
     */
    void request();

    /**
     * Cancels execution.
     */
    void cancel();

    /**
     * Resets execution sub-tree to initial state.
     */
    void reset();
}
