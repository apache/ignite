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

package org.apache.ignite.ml.optimization.updatecalculators;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;

/**
 * Data needed for RProp updater.
 * <p>
 * See <a href="https://paginas.fe.up.pt/~ee02162/dissertacao/RPROP%20paper.pdf">RProp</a>.</p>
 */
public class RPropParameterUpdate implements Serializable {
    /** Sums updates returned by different trainings. */
    public static final IgniteFunction<List<RPropParameterUpdate>, RPropParameterUpdate> SUM = RPropParameterUpdate::sum;

    /** Averages updates returned by different trainings. */
    public static final IgniteFunction<List<RPropParameterUpdate>, RPropParameterUpdate> AVG = RPropParameterUpdate::avg;

    /** Sums updates during one training. */
    public static final IgniteFunction<List<RPropParameterUpdate>, RPropParameterUpdate> SUM_LOCAL = RPropParameterUpdate::sumLocal;

    /** */
    private static final long serialVersionUID = -165584242642323332L;

    /**
     * Previous iteration parameters updates. In original paper they are labeled with "delta w".
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
     * Updates mask (values by which updateCache is multiplied).
     */
    protected Vector updatesMask;

    /**
     * Construct RPropParameterUpdate.
     *
     * @param paramsCnt Parameters count.
     * @param initUpdate Initial updateCache (in original work labeled as "delta_0").
     */
    RPropParameterUpdate(int paramsCnt, double initUpdate) {
        prevIterationUpdates = new DenseVector(paramsCnt);
        prevIterationGradient = new DenseVector(paramsCnt);
        deltas = new DenseVector(paramsCnt).assign(initUpdate);
        updatesMask = new DenseVector(paramsCnt);
    }

    /**
     * Construct instance of this class by given parameters.
     *
     * @param prevIterationUpdates Previous iteration parameters updates.
     * @param prevIterationGradient Previous iteration model partial derivatives by parameters.
     * @param deltas Previous iteration parameters deltas.
     * @param updatesMask Updates mask.
     */
    public RPropParameterUpdate(Vector prevIterationUpdates, Vector prevIterationGradient,
        Vector deltas, Vector updatesMask) {
        this.prevIterationUpdates = prevIterationUpdates;
        this.prevIterationGradient = prevIterationGradient;
        this.deltas = deltas;
        this.updatesMask = updatesMask;
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
    private RPropParameterUpdate setPrevIterationUpdates(Vector updates) {
        prevIterationUpdates = updates;

        return this;
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
    private RPropParameterUpdate setPrevIterationGradient(Vector gradient) {
        prevIterationGradient = gradient;
        return this;
    }

    /**
     * Get updates mask (values by which updateCache is multiplied).
     *
     * @return Updates mask (values by which updateCache is multiplied).
     */
    public Vector updatesMask() {
        return updatesMask;
    }

    /**
     * Set updates mask (values by which updateCache is multiplied).
     *
     * @param updatesMask New updatesMask.
     * @return This object.
     */
    public RPropParameterUpdate setUpdatesMask(Vector updatesMask) {
        this.updatesMask = updatesMask;

        return this;
    }

    /**
     * Set previous iteration deltas.
     *
     * @param deltas New deltas.
     * @return This object.
     */
    public RPropParameterUpdate setDeltas(Vector deltas) {
        this.deltas = deltas;

        return this;
    }

    /**
     * Sums updates during one training.
     *
     * @param updates Updates.
     * @return Sum of updates during one training.
     */
    private static RPropParameterUpdate sumLocal(List<RPropParameterUpdate> updates) {
        List<RPropParameterUpdate> nonNullUpdates = updates.stream().filter(Objects::nonNull)
            .collect(Collectors.toList());

        if (nonNullUpdates.isEmpty())
            return null;

        Vector newDeltas = nonNullUpdates.get(nonNullUpdates.size() - 1).deltas();
        Vector newGradient = nonNullUpdates.get(nonNullUpdates.size() - 1).prevIterationGradient();
        Vector totalUpdate = nonNullUpdates.stream().map(pu -> VectorUtils.elementWiseTimes(pu.updatesMask().copy(),
            pu.prevIterationUpdates())).reduce(Vector::plus).orElse(null);

        return new RPropParameterUpdate(totalUpdate, newGradient, newDeltas,
            new DenseVector(newDeltas.size()).assign(1.0));
    }

    /**
     * Sums updates returned by different trainings.
     *
     * @param updates Updates.
     * @return Sum of updates during returned by different trainings.
     */
    private static RPropParameterUpdate sum(List<RPropParameterUpdate> updates) {
        Vector totalUpdate = updates.stream().filter(Objects::nonNull)
            .map(pu -> VectorUtils.elementWiseTimes(pu.updatesMask().copy(), pu.prevIterationUpdates()))
            .reduce(Vector::plus).orElse(null);
        Vector totalDelta = updates.stream().filter(Objects::nonNull)
            .map(RPropParameterUpdate::deltas).reduce(Vector::plus).orElse(null);
        Vector totalGradient = updates.stream().filter(Objects::nonNull)
            .map(RPropParameterUpdate::prevIterationGradient).reduce(Vector::plus).orElse(null);

        if (totalUpdate != null)
            return new RPropParameterUpdate(totalUpdate, totalGradient, totalDelta,
                new DenseVector(Objects.requireNonNull(totalDelta).size()).assign(1.0));

        return null;
    }

    /**
     * Averages updates returned by different trainings.
     *
     * @param updates Updates.
     * @return Averages of updates during returned by different trainings.
     */
    private static RPropParameterUpdate avg(List<RPropParameterUpdate> updates) {
        List<RPropParameterUpdate> nonNullUpdates = updates.stream()
            .filter(Objects::nonNull).collect(Collectors.toList());
        int size = nonNullUpdates.size();

        RPropParameterUpdate sum = sum(updates);
        if (sum != null)
            return sum.
                setPrevIterationGradient(sum.prevIterationGradient().divide(size)).
                setPrevIterationUpdates(sum.prevIterationUpdates().divide(size)).
                setDeltas(sum.deltas().divide(size));

        return null;
    }
}
