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

package org.apache.ignite.ml.composition.boosting.convergence;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Before;

/** */
public abstract class ConvergenceCheckerTest {
    /** Not converged model. */
    protected ModelsComposition notConvergedMdl = new ModelsComposition(Collections.emptyList(), null) {
        @Override public Double apply(Vector features) {
            return 2.1 * features.get(0);
        }
    };

    /** Converged model. */
    protected ModelsComposition convergedMdl = new ModelsComposition(Collections.emptyList(), null) {
        @Override public Double apply(Vector features) {
            return 2 * (features.get(0) + 1);
        }
    };

    /** Features extractor. */
    protected IgniteBiFunction<double[], Double, Vector> fExtr = (x, y) -> VectorUtils.of(x);

    /** Label extractor. */
    protected IgniteBiFunction<double[], Double, Double> lbExtr = (x, y) -> y;

    /** Data. */
    protected Map<double[], Double> data;

    /** */
    @Before
    public void setUp() throws Exception {
        data = new HashMap<>();
        for(int i = 0; i < 10; i ++)
            data.put(new double[]{i, i + 1}, (double)(2 * (i + 1)));
    }

    /** */
    public ConvergenceChecker<double[], Double> createChecker(ConvergenceCheckerFactory factory,
        LocalDatasetBuilder<double[], Double> datasetBuilder) {

        return factory.create(data.size(),
            x -> x,
            new Loss() {
                @Override public double error(long sampleSize, double lb, double mdlAnswer) {
                    return mdlAnswer - lb;
                }

                @Override public double gradient(long sampleSize, double lb, double mdlAnswer) {
                    return mdlAnswer - lb;
                }
            },
            datasetBuilder, fExtr, lbExtr
        );
    }
}
