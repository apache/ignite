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

package org.apache.ignite.ml.selection.paramgrid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Generates tuples of hyper parameter values by given map.
 *
 * In the given map keys are names of hyper parameters
 * and values are arrays of values for hyper parameter presented in the key.
 */
public class ParameterSetGenerator {
    /** Size of parameter vector. Default value is 100. */
    private int sizeOfParamVector = 100;

    /** Params. */
    private List<Double[]> params = new ArrayList<>();

    /** The given map of hyper parameters and its values. */
    private Map<Integer, Double[]> map;

    /**
     * Creates an instance of the generator.
     *
     * @param map In the given map keys are names of hyper parameters
     * and values are arrays of values for hyper parameter presented in the key.
     */
    public ParameterSetGenerator(Map<Integer, Double[]> map) {
        assert map != null;
        assert !map.isEmpty();

        this.map = map;
        this.sizeOfParamVector = map.size();
    }

    /**
     * Returns the list of tuples presented as arrays.
     */
    public List<Double[]> generate() {

        Double[] nextPnt = new Double[sizeOfParamVector];

        traverseTree(map, nextPnt, -1);

        return params;
    }

    /**
     * Traverse tree on the current level and starts procedure of child traversing.
     *
     * @param map The current state of the data.
     * @param nextPnt Next point.
     * @param dimensionNum Dimension number.
     */
    private void traverseTree(Map<Integer, Double[]> map, Double[] nextPnt, int dimensionNum) {
        dimensionNum++;

        if (dimensionNum == sizeOfParamVector){
            Double[] paramSet = Arrays.copyOf(nextPnt, sizeOfParamVector);
            System.out.println(Arrays.toString(paramSet));
            params.add(paramSet);
            return;
        }

        Double[] valuesOfCurrDimension = map.get(dimensionNum);

        for (Double specificValue : valuesOfCurrDimension) {
            nextPnt[dimensionNum] = specificValue;
            traverseTree(map, nextPnt, dimensionNum);
        }
    }
}
