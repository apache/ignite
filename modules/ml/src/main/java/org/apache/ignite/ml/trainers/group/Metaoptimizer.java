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

import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Class encapsulating data transformations in group training in {@link MetaoptimizerGroupTrainer}, which is adapter of
 * {@link GroupTrainer}
 *
 * @param <LC> Local context of {@link GroupTrainer}.
 * @param <X> Type of data which is processed in training loop step.
 * @param <Y> Type of data returned by training loop step data processor.
 * @param <I> Type of data to which data returned by distributed initialization is mapped.
 * @param <D> Type of data returned by initialization.
 * @param <O> Type of data to which data returned by data processor is mapped.
 */
public interface Metaoptimizer<LC, X, Y, I, D, O> {
    /**
     * Get function used to reduce distributed initialization results.
     *
     * @return Function used to reduce distributed initialization results.
     */
    IgniteBinaryOperator<D> initialReducer();

    /**
     * Maps data returned by distributed initialization to data consumed by training loop step.
     *
     * @param data Data returned by distributed initialization.
     * @param locCtx Local context.
     * @return Mapping of data returned by distributed initialization to data consumed by training loop step.
     */
    I locallyProcessInitData(D data, LC locCtx);

    /**
     * Preprocess data for {@link MetaoptimizerGroupTrainer#dataProcessor()}.
     *
     * @return Preprocessed data for {@link MetaoptimizerGroupTrainer#dataProcessor()}.
     */
    default IgniteFunction<X, X> distributedPreprocessor() {
        return x -> x;
    }

    /**
     * Get function used to map values returned by {@link MetaoptimizerGroupTrainer#dataProcessor()}.
     *
     * @return Function used to map values returned by {@link MetaoptimizerGroupTrainer#dataProcessor()}.
     */
    IgniteFunction<Y, O> distributedPostprocessor();

    /**
     * Get binary operator used for reducing results returned by distributedPostprocessor.
     *
     * @return Binary operator used for reducing results returned by distributedPostprocessor.
     */
    IgniteBinaryOperator<O> postProcessReducer();

    /**
     * Get identity of postProcessReducer.
     *
     * @return Identity of postProcessReducer.
     */
    O postProcessIdentity();

    /**
     * Transform data returned by distributed part of training loop step into input fed into distributed part of training
     * loop step.
     *
     * @param input Type of output of distributed part of training loop step.
     * @param locCtx Local context.
     * @return Result of transform data returned by distributed part of training loop step into input fed into distributed part of training
     * loop step.
     */
    I localProcessor(O input, LC locCtx);

    /**
     * Returns value of predicate 'should training loop continue given previous step output and local context'.
     *
     * @param input Input of previous step.
     * @param locCtx Local context.
     * @return Value of predicate 'should training loop continue given previous step output and local context'.
     */
    boolean shouldContinue(I input, LC locCtx);

//    default <I1, D1, O1> Metaoptimizer<IR, LC, X, Y, IgniteBiTuple<I, I1>, IgniteBiTuple<D, D1>, IgniteBiTuple<O, O1>> combineWith(Metaoptimizer<IR, LC, X, Y, I1, D1, O1> other) {
//        Metaoptimizer<IR, LC, X, Y, I, D, O> me = this;
//        return new Metaoptimizer<IR, LC, X, Y, IgniteBiTuple<I, I1>, IgniteBiTuple<D, D1>, IgniteBiTuple<O, O1>>() {
//            @Override public IgniteBiTuple<D, D1> initialDistributedPostProcess(IR initData) {
//                return new IgniteBiTuple<>(me.initialDistributedPostProcess(initData), other.initialDistributedPostProcess(initData));
//            }
//
//            @Override public IgniteBiTuple<D, D1> initialReducer(IgniteBiTuple<D, D1> arg1, IgniteBiTuple<D, D1> arg2) {
//                return new IgniteBiTuple<>(me.initialReducer(arg1.get1(), arg2.get1()), other.initialReducer(arg1.get2(), arg2.get2()));
//            }
//
//            @Override public IgniteBiTuple<I, I1> locallyProcessInitData(IgniteBiTuple<D, D1> data, LC locCtx) {
//                return new IgniteBiTuple<>(me.locallyProcessInitData(data.get1(), locCtx), other.locallyProcessInitData(data.get2(), locCtx));
//            }
//
//            @Override public X distributedPreprocessor(X dataToProc) {
//                return other.distributedPreprocessor(me.distributedPreprocessor(dataToProc));
//            }
//
//            @Override public IgniteBiTuple<O, O1> distributedPostprocessor(Y procOut) {
//                return new IgniteBiTuple<>(me.distributedPostprocessor(procOut), other.distributedPostprocessor(procOut));
//            }
//
//            @Override
//            public IgniteBiTuple<O, O1> postProcessReducer(IgniteBiTuple<O, O1> arg1, IgniteBiTuple<O, O1> arg2) {
//                return new IgniteBiTuple<>(me.postProcessReducer(arg1.get1(), arg2.get1()), other.postProcessReducer(arg1.get2(), arg2.get2()));
//            }
//
//            @Override public IgniteBiTuple<O, O1> postProcessIdentity() {
//                return new IgniteBiTuple<>(me.postProcessIdentity(), other.postProcessIdentity());
//            }
//
//            @Override public IgniteBiTuple<I, I1> localProcessor(IgniteBiTuple<O, O1> input, LC locCtx) {
//                return new IgniteBiTuple<>(me.localProcessor(input.get1(), locCtx), other.localProcessor(input.get2(), locCtx));
//            }
//
//            @Override public boolean shouldContinue(IgniteBiTuple<I, I1> input, LC locCtx) {
//                return me.shouldContinue(input.get1(), locCtx) && other.shouldContinue(input.get2(), locCtx);
//            }
//        };
//    }
}
