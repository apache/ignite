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

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteFunction;

public class AdaptableDatasetModel<I, O, IW, OW, M extends Model<IW, OW>> implements Model<I, O> {
    private final IgniteFunction<I, IW> before;
    private final IgniteFunction<OW, O> after;
    private final M mdl;

    public AdaptableDatasetModel(IgniteFunction<I, IW> before, M mdl, IgniteFunction<OW, O> after) {
        this.before = before;
        this.after = after;
        this.mdl = mdl;
    }

    @Override public O apply(I i) {
        return before.andThen(mdl).andThen(after).apply(i);
    }

    @Override public <O1> AdaptableDatasetModel<I, O1, IW, OW, M> andThen(IgniteFunction<O, O1> after) {
        return new AdaptableDatasetModel<>(before, mdl, i -> after.apply(this.after.apply(i)));
    }

    public <I1> AdaptableDatasetModel<I1, O, IW, OW, M> andBefore(IgniteFunction<I1, I> before) {
        IgniteFunction<I1, IW> function = i -> this.before.apply(before.apply(i));
        return new AdaptableDatasetModel<>(function, mdl, after);
    }

    public M innerModel() {
        return mdl;
    }

    public <M1 extends Model<IW, OW>> AdaptableDatasetModel<I, O, IW, OW, M1> withInnerModel(M1 mdl) {
        return new AdaptableDatasetModel<>(before, mdl, after);
    }
}
