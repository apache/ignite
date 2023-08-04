/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package de.kp.works.ignite.gremlin.process.computer;

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class TinkerMessageBoard<M> {

    public Map<MessageScope, Map<Vertex,Queue<M>>> sendMessages = new ConcurrentHashMap<>();
    public Map<MessageScope, Map<Vertex, Queue<M>>> receiveMessages = new ConcurrentHashMap<>();
    public Set<MessageScope> previousMessageScopes = new HashSet<>();
    public Set<MessageScope> currentMessageScopes = new HashSet<>();

    public void completeIteration() {
        this.receiveMessages = this.sendMessages;
        this.sendMessages = new ConcurrentHashMap<>();
        this.previousMessageScopes = this.currentMessageScopes;
        this.currentMessageScopes = new HashSet<>();
    }
}
