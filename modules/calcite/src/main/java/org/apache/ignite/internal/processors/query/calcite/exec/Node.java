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
 */
public interface Node<T> extends Source {
    /**
     * Requests a target sink of the node. The sink is used to push data into the node by its children.
     *
     * @param idx Sink index.
     * @return Sink object.
     * @throws IndexOutOfBoundsException in case there is no Sink object associated with given index.
     */
    Sink<T> sink(int idx);

    /**
     * Registers sources of this node. Sources are used to notify children when the node is ready to consume data.
     *
     * @param sources Sources.
     */
    void sources(List<Source> sources);
}
