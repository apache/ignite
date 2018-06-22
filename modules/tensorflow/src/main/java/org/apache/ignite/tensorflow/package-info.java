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

/**
 * <!-- Package description. -->
 * TensorFlow integration that allows to start and maintain TensorFlow cluster on top of Apache Ignite cluster
 * infrastructure. The TensorFlow cluster is built for the specified cache the following way:
 * <ol>
 *     <li>The TensorFlow cluster maintainer is created to maintain the cluster associated with the specified cache so
 *     that this service works reliable even the node will fail. It's achieved using Ignite Service Grid.</li>
 *     <li>TensorFlow cluster maintainer finds out that cluster is not started and begins starting procedure.</li>
 *     <li>TensorFlow cluster resolver builds cluster specification based on the specified cache so that every
 *     TensorFlow task is associated with partition of the cache and assumed to be started on the node where the
 *     partition is kept.</li>
 *     <li>Based on the built cluster specification the set of tasks is sent to the nodes on purpose to start TensorFlow
 *     servers on the nodes defined in the specification.</li>
 *     <li>When this set of tasks is completed successfully the started process identifiers are returned and saved
 *     for future using.</li>
 *     <li>The starting procedure is completed. In case a server fails the cluster will be turn down and then started
 *     again by maintainer.</li>
 * </ol>
 */
package org.apache.ignite.tensorflow;