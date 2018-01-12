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

import java.util.List;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Class encapsulating data transformations in group training in {@link MetaoptimizerGroupTrainer}, which is adapter of
 * {@link GroupTrainer}.
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
    IgniteFunction<List<D>, D> initialReducer();

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
    IgniteFunction<List<O>, O> postProcessReducer();

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
}
