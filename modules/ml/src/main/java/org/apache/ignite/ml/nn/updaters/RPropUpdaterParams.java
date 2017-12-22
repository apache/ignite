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

package org.apache.ignite.ml.nn.updaters;

import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorUtils;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

/**
 * Data needed for RProp updater.
 * @see <a href="https://paginas.fe.up.pt/~ee02162/dissertacao/RPROP%20paper.pdf">https://paginas.fe.up.pt/~ee02162/dissertacao/RPROP%20paper.pdf</a>.
 */
public class RPropUpdaterParams implements UpdaterParams<SmoothParametrized> {
    /**
     * Previous iteration weights updates. In original paper they are labeled with "delta w".
     */
    protected Vector prevIterationUpdates;

    /**
     * Previous iteration model partial derivatives by parameters.
     */
    protected Vector prevIterationGradient;
    /**
     * Previous iteration parameters deltas. In original paper they are labeled with "delta".
     */
    protected Vector deltas;

    /**
     * Updates mask (values by which update is multiplied).
     */
    protected Vector updatesMask;

    /**
     * Construct RPropUpdaterParams.
     *
     * @param paramsCount Parameters count.
     * @param initUpdate Initial update (in original work labeled as "delta_0").
     */
    RPropUpdaterParams(int paramsCount, double initUpdate) {
        prevIterationUpdates = new DenseLocalOnHeapVector(paramsCount);
        prevIterationGradient = new DenseLocalOnHeapVector(paramsCount);
        deltas = new DenseLocalOnHeapVector(paramsCount).assign(initUpdate);
        updatesMask = new DenseLocalOnHeapVector(paramsCount);
    }

    /**
     * Get bias deltas.
     *
     * @return Bias deltas.
     */
    Vector deltas() {
        return deltas;
    }

    /**
     * Get previous iteration biases updates. In original paper they are labeled with "delta w".
     *
     * @return Biases updates.
     */
    Vector prevIterationUpdates() {
        return prevIterationUpdates;
    }

    /**
     * Set previous iteration parameters updates. In original paper they are labeled with "delta w".
     *
     * @param updates New parameters updates value.
     * @return This object.
     */
    Vector setPrevIterationBiasesUpdates(Vector updates) {
        return prevIterationUpdates = updates;
    }

    /**
     * Get previous iteration loss function partial derivatives by parameters.
     *
     * @return Previous iteration loss function partial derivatives by parameters.
     */
    Vector prevIterationGradient() {
        return prevIterationGradient;
    }

    /**
     * Set previous iteration loss function partial derivatives by parameters.
     *
     * @return This object.
     */
    RPropUpdaterParams setPrevIterationWeightsDerivatives(Vector gradient) {
        prevIterationGradient = gradient;
        return this;
    }

    /**
     * Get updates mask (values by which update is multiplied).
     *
     * @return Updates mask (values by which update is multiplied).
     */
    public Vector updatesMask() {
        return updatesMask;
    }

    /**
     * Set updates mask (values by which update is multiplied).
     *
     * @param updatesMask New updatesMask.
     */
    public RPropUpdaterParams setUpdatesMask(Vector updatesMask) {
        this.updatesMask = updatesMask;

        return this;
    }

    /** {@inheritDoc} */
    @Override public <M extends SmoothParametrized> M update(M obj) {
        Vector updatesToAdd = VectorUtils.elementWiseTimes(updatesMask.copy(), prevIterationUpdates);
        return (M)obj.setParameters(obj.parameters().plus(updatesToAdd));
    }
}
