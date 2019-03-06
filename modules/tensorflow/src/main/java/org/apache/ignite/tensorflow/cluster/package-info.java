/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

/**
 * <!-- Package description. -->
 * TensorFlow integration API that allows to start and maintain TensorFlow cluster using infrastructure tools from
 * package {@link org.apache.ignite.tensorflow.core}. The most important components are:
 * <ul>
 *     <li>{@link org.apache.ignite.tensorflow.cluster.TensorFlowClusterManager} that allows to start and stop
 *     TensorFlow cluster on top of Apache Ignite, but doesn't monitor it and doesn't maintain so that in case of
 *     failure the cluster won't be restarted.</li>
 *     <li>{@link org.apache.ignite.tensorflow.cluster.TensorFlowClusterGatewayManager} that allows to start, maintain
 *     and stop TensorFlow cluster on top of Apache Ignite so that in case of failure the cluster will be restarted and
 *     recovered.</li>
 *     <li>{@link org.apache.ignite.tensorflow.cluster.TensorFlowClusterGateway} that allows to subscribe on cluster
 *     configuration changes that might be done as result of rebalancing or node failures.</li>
 * </ul>
 */
package org.apache.ignite.tensorflow.cluster;