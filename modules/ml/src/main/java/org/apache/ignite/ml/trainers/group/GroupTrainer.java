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

import java.util.Collection;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.trainers.Trainer;

public abstract class GroupTrainer<I, O, R, C, M extends Model, T> implements Trainer<M, T> {
    IgniteCluster cluster;
    IgniteBiFunction<Integer, T, O> init;
    IgniteFunction<I, O> worker;
    IgniteFunction<I, IgniteBiTuple<C, R>> handler;
    IgnitePredicate<Collection<O>> stopper;
    IgniteFunction<Integer, R> result;
    IgniteFunction<Collection<R>, M> modelProducer;

    public GroupTrainer(IgniteCluster cluster, IgniteBiFunction<Integer, T, O> init, IgniteFunction<I, O> worker,
        IgniteFunction<I, IgniteBiTuple<C, R>> handler,
        IgnitePredicate<Collection<O>> stopper,
        IgniteFunction<Integer, R> result,
        IgniteFunction<Collection<R>, M> modelProducer) {
        this.cluster = cluster;
        this.init = init;
        this.worker = worker;
        this.handler = handler;
        this.stopper = stopper;
        this.result = result;
        this.modelProducer = modelProducer;
    }

    @Override public M train(T data) {
        return null;
    }
}
