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

package org.apache.ignite.ml.nn;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;

/**
 * State of MLP after computation.
 */
public class MLPState {
    /**
     * Output of activators.
     */
    protected List<Matrix> activatorsOutput;

    /**
     * Output of linear transformations.
     */
    protected List<Matrix> linearOutput;

    /**
     * Input.
     */
    protected Matrix input;

    /**
     * Construct MLP state.
     *
     * @param input Matrix of inputs.
     */
    public MLPState(Matrix input) {
        this.input = input != null ? input.copy() : null;
        linearOutput = new ArrayList<>();
        activatorsOutput = new ArrayList<>();
    }

    /**
     * Output of activators of given layer. If layer index is 0, inputs are returned.
     *
     * @param layer Index of layer to get activators outputs from.
     * @return Activators output.
     */
    public Matrix activatorsOutput(int layer) {
        return layer > 0 ? activatorsOutput.get(layer - 1) : input;
    }

    /**
     * Output of linear transformation of given layer. If layer index is 0, inputs are returned.
     *
     * @param layer Index of layer to get linear transformation outputs from.
     * @return Linear transformation output.
     */
    public Matrix linearOutput(int layer) {
        return layer == 0 ? input : linearOutput.get(layer - 1);
    }
}
