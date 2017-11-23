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

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.trainers.group.chain.DistributedTrainerWorkersChain;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.trainers.group.chain.HasTrainingUUID;

public abstract class SimpleGroupTrainer<LC extends HasTrainingUUID, K, V, IR extends Serializable,
R extends Serializable, I extends Serializable,
M extends Model, T extends Distributive<K>,
G, O extends Serializable, D> extends
    GroupTrainer<LC, K, V, IR, R, I, M, T, G> {
    private Metaoptimizer<IR, I, D, O, G> metaoptimizer;

    public SimpleGroupTrainer(
        IgniteCache<GroupTrainerCacheKey<K>, V> cache,
        Ignite ignite) {
        super(cache, ignite);
    }

    protected DistributedTrainerWorkersChain<LC, K, V, I, GroupTrainingContext<K, V, LC>, I> trainingLoopStep() {
        // TODO: Implement
        return null;
    }

//    default <O1 extends Serializable, G> DistributedTrainerWorkersChain<L, K, V, I, C, O1> thenDistributedForEntries(
//        IgniteBiFunction<O, L, G> remoteCtxExtractor,
//        IgniteTriFunction<O, L, EntryAndContext<K, V, G>, ResultAndUpdates<O1>> distributedWorker,
//        IgniteBiFunction<O, L, IgniteSupplier<Stream<GroupTrainerCacheKey<K>>>> kf,
//        O1 identity,

    protected abstract O, L, EntryAndContext<K, V, G>, ResultAndUpdates<O1>
}
