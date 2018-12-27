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

package org.apache.ignite.ml.composition.stacking;

import org.apache.ignite.ml.Model;

/**
 * This is just a wrapper class for model produced by {@link StackedDatasetTrainer}.
 *
 * @param <IS>
 * @param <IA>
 * @param <O>
 * @param <AM>
 */
public class StackedModel<IS, IA, O, AM extends Model<IA, O>> implements Model<IS, O> {
    private Model<IS, O> mdl;

    StackedModel(Model<IS, O> mdl) {
        this.mdl = mdl;
    }

    /** {@inheritDoc} */
    @Override public O apply(IS is) {
        return mdl.apply(is);
    }
}
