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
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;

/**
 * Parameters for {@link SimpleGDUpdateCalculator}.
 */
public class SimpleGDParameterUpdate implements Serializable {
    /** Method used to sum updates inside of one of parallel trainings. */
    public static final IgniteFunction<List<SimpleGDParameterUpdate>, SimpleGDParameterUpdate> AVG = SimpleGDParameterUpdate::avg;

    /** Method used to get total update of all parallel trainings. */
    public static final IgniteFunction<List<SimpleGDParameterUpdate>, SimpleGDParameterUpdate> SUM_LOCAL = SimpleGDParameterUpdate::sumLocal;

    /** */
    private static final long serialVersionUID = -8732955283436005621L;

    /** Gradient. */
    private Vector gradient;

    /**
     * Construct instance of this class.
     *
     * @param paramsCnt Count of parameters.
     */
    public SimpleGDParameterUpdate(int paramsCnt) {
        gradient = new DenseVector(paramsCnt);
    }

    /**
     * Construct instance of this class.
     *
     * @param gradient Gradient.
     */
    public SimpleGDParameterUpdate(Vector gradient) {
        this.gradient = gradient;
    }

    /**
     * Get gradient.
     *
     * @return Get gradient.
     */
    public Vector gradient() {
        return gradient;
    }

    /**
     * Method used to sum updates inside of one of parallel trainings.
     *
     * @param updates Updates.
     * @return Sum of SimpleGDParameterUpdate.
     */
    private static SimpleGDParameterUpdate sumLocal(List<SimpleGDParameterUpdate> updates) {
        Vector accumulatedGrad = updates.
            stream().
            filter(Objects::nonNull).
            map(SimpleGDParameterUpdate::gradient).
            reduce(Vector::plus).
            orElse(null);

        return accumulatedGrad != null ? new SimpleGDParameterUpdate(accumulatedGrad) : null;
    }

    /**
     * Method used to get total update of all parallel trainings.
     *
     * @param updates Updates.
     * @return Avg of SimpleGDParameterUpdate.
     */
    private static SimpleGDParameterUpdate avg(List<SimpleGDParameterUpdate> updates) {
        SimpleGDParameterUpdate sum = sumLocal(updates);
        return sum != null ? new SimpleGDParameterUpdate(sum.gradient().
            divide(updates.stream().filter(Objects::nonNull).collect(Collectors.toList()).size())) : null;
    }
}
