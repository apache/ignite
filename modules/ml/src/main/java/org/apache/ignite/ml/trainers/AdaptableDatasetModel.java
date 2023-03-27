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

package org.apache.ignite.ml.trainers;

import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Model which is composition of form {@code before `andThen` inner Mdl `andThen` after}.
 *
 * @param <I> Type of input of this model.
 * @param <O> Type of output of this model.
 * @param <IW> Type of input of inner model.
 * @param <OW> Type of output of inner model.
 * @param <M> Type of inner model.
 */
public class AdaptableDatasetModel<I, O, IW, OW, M extends IgniteModel<IW, OW>> implements IgniteModel<I, O> {
    /** Function applied before inner model. */
    private final IgniteFunction<I, IW> before;

    /** Function applied after inner model.*/
    private final IgniteFunction<OW, O> after;

    /** Inner model. */
    private final M mdl;

    /**
     * Construct instance of this class.
     *
     * @param before Function applied before wrapped model.
     * @param mdl Inner model.
     * @param after Function applied after wrapped model.
     */
    public AdaptableDatasetModel(IgniteFunction<I, IW> before, M mdl, IgniteFunction<OW, O> after) {
        this.before = before;
        this.after = after;
        this.mdl = mdl;
    }

    /**
     * Result of this model application is a result of composition {@code before `andThen` inner mdl `andThen` after}.
     */
    @Override public O predict(I i) {
        return before.andThen(mdl::predict).andThen(after).apply(i);
    }

    /** {@inheritDoc} */
    @Override public <O1> AdaptableDatasetModel<I, O1, IW, OW, M> andThen(IgniteModel<O, O1> after) {
        return new AdaptableDatasetModel<>(before, mdl, i -> after.predict(this.after.apply(i)));
    }

    /**
     * Create new {@code AdaptableDatasetModel} which is a composition of the form {@code thisMdl . before}.
     *
     * @param before Function applied before this model.
     * @param <I1> Type of function applied before this model.
     * @return New {@code AdaptableDatasetModel} which is a composition of the form {@code thisMdl . before}.
     */
    @Override public <I1> AdaptableDatasetModel<I1, O, IW, OW, M> andBefore(IgniteFunction<I1, I> before) {
        IgniteFunction<I1, IW> function = i -> this.before.apply(before.apply(i));
        return new AdaptableDatasetModel<>(function, mdl, after);
    }

    /**
     * Get inner model.
     *
     * @return Inner model.
     */
    public M innerModel() {
        return mdl;
    }

    /**
     * Create new instance of this class with changed inner model.
     *
     * @param mdl Inner model.
     * @param <M1> Type of inner model.
     * @return New instance of this class with changed inner model.
     */
    public <M1 extends IgniteModel<IW, OW>> AdaptableDatasetModel<I, O, IW, OW, M1> withInnerModel(M1 mdl) {
        return new AdaptableDatasetModel<>(before, mdl, after);
    }
}
