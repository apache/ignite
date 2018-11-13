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
 * TensorFlow integration core package that provides infrastructure layers that allows TensorFlow cluster to start and
 * be maintained. It provides layer hierarchy. The lowermost layer (long-running process layer) provides API to start
 * and maintain abstract long-running processes. The second layer (native-running processes layer) is built on top of
 * previous layer and allows to start and maintain native processes. And the third layer (python-running processes
 * layer) is responsible for starting and maintaining of Python native processes.
 */
package org.apache.ignite.tensorflow.core;