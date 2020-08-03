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

package org.apache.ignite.ml.selection.scoring.evaluator.context;

import java.io.Serializable;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Class represents context stub for metrics that don't require such context preparations.
 */
public class EmptyContext<L extends Serializable> implements EvaluationContext<L, EmptyContext<L>> {
    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 1439494372470803212L;

    /**
     * {@inheritDoc}
     */
    @Override public void aggregate(LabeledVector<L> vector) {

    }

    /**
     * {@inheritDoc}
     */
    @Override public EmptyContext<L> mergeWith(EmptyContext<L> other) {
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean needToCompute() {
        return false;
    }
}
