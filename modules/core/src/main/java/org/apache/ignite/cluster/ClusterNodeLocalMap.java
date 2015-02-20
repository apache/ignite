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

package org.apache.ignite.cluster;

import java.util.concurrent.*;

/**
 * Defines interface for node-local storage.
 * <p>
 * {@code GridNodeLocalMap} is similar to {@link ThreadLocal} in a way that its values are not
 * distributed and kept only on local node (similar like {@link ThreadLocal} values are attached to the
 * current thread only). Node-local values are used primarily by jobs executed from the remote
 * nodes to keep intermediate state on the local node between executions.
 * <p>
 * {@code GridNodeLocalMap} essentially is a {@link ConcurrentMap} with a few additional methods,
 * so it is fairly trivial to use.
 * <p>
 * You can get an instance of {@code GridNodeLocalMap} by calling {@link org.apache.ignite.IgniteCluster#nodeLocalMap()} method.
 */
public interface ClusterNodeLocalMap<K, V> extends ConcurrentMap<K, V> {
}
