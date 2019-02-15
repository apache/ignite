/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.optimization.updatecalculators;

import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.apache.ignite.ml.optimization.SmoothParametrized;

/**
 * Class encapsulating RProp algorithm.
 * <p>
 * See <a href="https://paginas.fe.up.pt/~ee02162/dissertacao/RPROP%20paper.pdf">RProp</a>.</p>
 */
public class RPropUpdateCalculator implements ParameterUpdateCalculator<SmoothParametrized, RPropParameterUpdate> {
    /** */
    private static final long serialVersionUID = -5156816330041409864L;

    /**
     * Default initial update.
     */
    private static double DFLT_INIT_UPDATE = 0.1;

    /**
     * Default acceleration rate.
     */
    private static double DFLT_ACCELERATION_RATE = 1.2;

    /**
     * Default deacceleration rate.
     */
    private static double DFLT_DEACCELERATION_RATE = 0.5;

    /**
     * Initial update.
     */
    private final double initUpdate;

    /**
     * Acceleration rate.
     */
    private final double accelerationRate;

    /**
     * Deacceleration rate.
     */
    private final double deaccelerationRate;

    /**
     * Maximal value for update.
     */
    private static final double UPDATE_MAX = 50.0;

    /**
     * Minimal value for update.
     */
    private static final double UPDATE_MIN = 1E-6;

    /**
     * Loss function.
     */
    protected IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    /**
     * Construct RPropUpdateCalculator.
     *
     * @param initUpdate Initial update.
     * @param accelerationRate Acceleration rate.
     * @param deaccelerationRate Deacceleration rate.
     */
    public RPropUpdateCalculator(double initUpdate, double accelerationRate, double deaccelerationRate) {
        this.initUpdate = initUpdate;
        this.accelerationRate = accelerationRate;
        this.deaccelerationRate = deaccelerationRate;
    }

    /**
     * Construct RPropUpdateCalculator with default parameters.
     */
    public RPropUpdateCalculator() {
        this(DFLT_INIT_UPDATE, DFLT_ACCELERATION_RATE, DFLT_DEACCELERATION_RATE);
    }

    /** {@inheritDoc} */
    @Override public RPropParameterUpdate calculateNewUpdate(SmoothParametrized mdl, RPropParameterUpdate updaterParams,
        int iteration, Matrix inputs, Matrix groundTruth) {
        Vector gradient = mdl.differentiateByParameters(loss, inputs, groundTruth);
        Vector prevGradient = updaterParams.prevIterationGradient();
        Vector derSigns;

        if (prevGradient != null)
            derSigns = VectorUtils.zipWith(prevGradient, gradient, (x, y) -> Math.signum(x * y));
        else
            derSigns = gradient.like(gradient.size()).assign(1.0);

        Vector newDeltas = updaterParams.deltas().copy().map(derSigns, (prevDelta, sign) -> {
            if (sign > 0)
                return Math.min(prevDelta * accelerationRate, UPDATE_MAX);
            else if (sign < 0)
                return Math.max(prevDelta * deaccelerationRate, UPDATE_MIN);
            else
                return prevDelta;
        });

        Vector newPrevIterationUpdates = MatrixUtil.zipWith(gradient, updaterParams.deltas(), (der, delta, i) -> {
            if (derSigns.getX(i) >= 0)
                return -Math.signum(der) * delta;

            return updaterParams.prevIterationUpdates().getX(i);
        });

        Vector updatesMask = MatrixUtil.zipWith(derSigns, updaterParams.prevIterationUpdates(), (sign, upd, i) -> {
            if (sign < 0)
                gradient.setX(i, 0.0);

            if (sign >= 0)
                return 1.0;
            else
                return -1.0;
        });

        return new RPropParameterUpdate(newPrevIterationUpdates, gradient.copy(), newDeltas, updatesMask);
    }

    /** {@inheritDoc} */
    @Override public RPropParameterUpdate init(SmoothParametrized mdl,
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        this.loss = loss;
        return new RPropParameterUpdate(mdl.parametersCount(), initUpdate);
    }

    /** {@inheritDoc} */
    @Override public <M1 extends SmoothParametrized> M1 update(M1 obj, RPropParameterUpdate update) {
        Vector updatesToAdd = VectorUtils.elementWiseTimes(update.updatesMask().copy(), update.prevIterationUpdates());
        return (M1)obj.setParameters(obj.parameters().plus(updatesToAdd));
    }
}
