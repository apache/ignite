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

import org.apache.ignite.lang.IgniteBiTuple;

public interface Metaoptimizer<IR, LC, X, I, D, O> {
    D initialDistributedPostProcess(IR res);

    D initialReducer(D arg1, D arg2);

    X distributedPreprocess(I inputData, X dataToProcess);

    O distributedPostprocess(X procOut);

    O postProcessReducer(O arg1, O arg2);

    I localProcessor(O input, LC locCtx);

    boolean shouldContinue(I input, LC locCtx);

    default <I1, D1, O1> Metaoptimizer<IR, LC, X, IgniteBiTuple<I, I1>, IgniteBiTuple<D, D1>, IgniteBiTuple<O, O1>> combineWith(Metaoptimizer<IR, LC, X, I1, D1, O1> other) {
        Metaoptimizer<IR, LC, X, I, D, O> me = this;
        return new Metaoptimizer<IR, LC, X, IgniteBiTuple<I, I1>, IgniteBiTuple<D, D1>, IgniteBiTuple<O, O1>>() {
            @Override public IgniteBiTuple<D, D1> initialDistributedPostProcess(IR initialData) {
                return new IgniteBiTuple<>(me.initialDistributedPostProcess(initialData), other.initialDistributedPostProcess(initialData));
            }

            @Override public IgniteBiTuple<D, D1> initialReducer(IgniteBiTuple<D, D1> arg1, IgniteBiTuple<D, D1> arg2) {
                return new IgniteBiTuple<>(me.initialReducer(arg1.get1(), arg2.get1()), other.initialReducer(arg1.get2(), arg2.get2()));
            }

            @Override public X distributedPreprocess(IgniteBiTuple<I, I1> inputData, X dataToProcess) {
                return other.distributedPreprocess(inputData.get2(), me.distributedPreprocess(inputData.get1(), dataToProcess));
            }

            @Override public IgniteBiTuple<O, O1> distributedPostprocess(X procOut) {
                return new IgniteBiTuple<>(me.distributedPostprocess(procOut), other.distributedPostprocess(procOut));
            }

            @Override
            public IgniteBiTuple<O, O1> postProcessReducer(IgniteBiTuple<O, O1> arg1, IgniteBiTuple<O, O1> arg2) {
                return new IgniteBiTuple<>(me.postProcessReducer(arg1.get1(), arg2.get1()), other.postProcessReducer(arg1.get2(), arg2.get2()));
            }

            @Override public IgniteBiTuple<I, I1> localProcessor(IgniteBiTuple<O, O1> input, LC locCtx) {
                return new IgniteBiTuple<>(me.localProcessor(input.get1(), locCtx), other.localProcessor(input.get2(), locCtx));
            }

            @Override public boolean shouldContinue(IgniteBiTuple<I, I1> input, LC locCtx) {
                return me.shouldContinue(input.get1(), locCtx) && other.shouldContinue(input.get2(), locCtx);
            }
        };
    }
}
