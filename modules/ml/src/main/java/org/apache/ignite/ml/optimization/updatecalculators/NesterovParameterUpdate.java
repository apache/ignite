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
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

/**
 * Data needed for Nesterov parameters updater.
 */
public class NesterovParameterUpdate implements Serializable {
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
        prevIterationUpdates = new DenseLocalOnHeapVector(paramsCnt).assign(0);
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
        return parameters.stream().filter(Objects::nonNull).map(NesterovParameterUpdate::prevIterationUpdates)
            .reduce(Vector::plus).map(NesterovParameterUpdate::new).orElse(null);
    }

    /**
     * Get average of parameters updates.
     *
     * @param parameters Parameters to average.
     * @return Average of parameters updates.
     */
    public static NesterovParameterUpdate avg(List<NesterovParameterUpdate> parameters) {
        NesterovParameterUpdate sum = sum(parameters);
        return sum != null ? sum.setPreviousUpdates(sum.prevIterationUpdates().divide(parameters.size())) : null;
    }
}
