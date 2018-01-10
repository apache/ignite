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

package org.apache.ignite.ml.trainers.group.chain;

import java.io.Serializable;

/**
 * {@link DistributedStep} specialized to {@link KeyAndContext}.
 *
 * @param <L> Local context.
 * @param <K> Type of keys of cache used for group training.
 * @param <C> Context used by worker.
 * @param <I> Type of input to this step.
 * @param <O> Type of output of this step.
 */
public interface DistributedKeyProcessingStep<L, K, C, I, O extends Serializable> extends
    DistributedStep<KeyAndContext<K, C>, L, K, C, I, O> {
}
