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

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;

/**
 * Data needed for Nesterov parameters updater.
 */
public class NesterovParameterUpdate implements Serializable {
    /** */
    private static final long serialVersionUID = -6370106062737202385L;

    /**
     * Previous step weights updates.
     */
    protected Vector prevIterationUpdates;

    /**
     * Construct NesterovParameterUpdate.
     *
     * @param paramsCnt Count of parameters on which updateCache happens.
     */
    public NesterovParameterUpdate(int paramsCnt) {
        prevIterationUpdates = new DenseVector(paramsCnt).assign(0);
    }

    /**
     * Construct NesterovParameterUpdate.
     *
     * @param prevIterationUpdates Previous iteration updates.
     */
    public NesterovParameterUpdate(Vector prevIterationUpdates) {
        this.prevIterationUpdates = prevIterationUpdates;
    }

    /**
     * Set previous step parameters updates.
     *
     * @param updates Parameters updates.
     * @return This object with updated parameters updates.
     */
    public NesterovParameterUpdate setPreviousUpdates(Vector updates) {
        prevIterationUpdates = updates;
        return this;
    }

    /**
     * Get previous step parameters updates.
     *
     * @return Previous step parameters updates.
     */
    public Vector prevIterationUpdates() {
        return prevIterationUpdates;
    }

    /**
     * Get sum of parameters updates.
     *
     * @param parameters Parameters to sum.
     * @return Sum of parameters updates.
     */
    public static NesterovParameterUpdate sum(List<NesterovParameterUpdate> parameters) {
        return parameters
            .stream()
            .filter(Objects::nonNull)
            .map(NesterovParameterUpdate::prevIterationUpdates)
            .reduce(Vector::plus)
            .map(NesterovParameterUpdate::new)
            .orElse(null);
    }

    /**
     * Get average of parameters updates.
     *
     * @param parameters Parameters to average.
     * @return Average of parameters updates.
     */
    public static NesterovParameterUpdate avg(List<NesterovParameterUpdate> parameters) {
        NesterovParameterUpdate sum = sum(parameters);
        return sum != null ? sum.setPreviousUpdates(sum.prevIterationUpdates()
            .divide(parameters.stream()
                .filter(Objects::nonNull).count())) : null;
    }
}
