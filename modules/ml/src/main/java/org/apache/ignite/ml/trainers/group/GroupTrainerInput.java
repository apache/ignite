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

package org.apache.ignite.ml.trainers.group;

import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteSupplier;

/**
 * Interface for {@link GroupTrainer} inputs.
 *
 * @param <K> Types of cache keys used for group training.
 */
public interface GroupTrainerInput<K> {
    /**
     * Get supplier of stream of keys used for initialization of {@link GroupTrainer}.
     *
     * @param trainingUUID UUID of training.
     * @return Supplier of stream of keys used for initialization of {@link GroupTrainer}.
     */
    IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> initialKeys(UUID trainingUUID);
}
