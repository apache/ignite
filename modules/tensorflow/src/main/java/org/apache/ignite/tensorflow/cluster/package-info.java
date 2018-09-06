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