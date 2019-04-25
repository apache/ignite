/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <!-- Package description. -->
 * The part of TensorFlow integration infrastructure that allows to start and maintain abstract native processes. As
 * described in {@link org.apache.ignite.tensorflow.core.nativerunning.NativeProcess} user only needs to specify process
 * builder, stdin and node identifier the process should be started on and Native Process Manager will make the rest so
 * that the specified process will be executed and maintained on the specified node.
 */
package org.apache.ignite.tensorflow.core.nativerunning;