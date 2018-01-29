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
import java.util.stream.Stream;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorUtils;

/**
 * Class encapsulating parameter update in RMSProp algorithm.
 */
public class RMSPropParameterUpdate implements Serializable {
    /** Running average. */
    private Vector squaresRunningAverage;

    /** Update. */
    private Vector update;

    /**
     * Create instance of this class given count of parameters of model.
     *
     * @param paramsCnt Count of parameters of trained model.
     */
    public RMSPropParameterUpdate(int paramsCnt) {
        squaresRunningAverage = VectorUtils.zeroes(paramsCnt);
        update = VectorUtils.zeroes(paramsCnt);
    }

    /**
     * Create instance of this class from given parameters.
     *
     * @param squaresRunningAverage Squares running average.
     * @param update Update.
     */
    public RMSPropParameterUpdate(Vector squaresRunningAverage, Vector update) {
        this.squaresRunningAverage = squaresRunningAverage;
        this.update = update;
    }

    /**
     * Get squares of gradient running average.
     *
     * @return Squares of gradient running average.
     */
    public Vector squaresRunningAverage() {
        return squaresRunningAverage;
    }

    /**
     * Get the update.
     *
     * @return Update.
     */
    public Vector update() {
        return update;
    }

    /**
     * Get average of updates.
     *
     * @param updates Updates.
     * @return Average of updates.
     */
    public static RMSPropParameterUpdate avg(List<RMSPropParameterUpdate> updates) {
        List<Vector> nonNull = updates.stream().map(RMSPropParameterUpdate::update).filter(Objects::nonNull).collect(Collectors.toList());
        if (!nonNull.isEmpty()) {
            Vector updatesSum = nonNull.stream().reduce(Vector::plus).orElse(null);
            Vector squaresRunningAverageSum = updates.stream().map(RMSPropParameterUpdate::squaresRunningAverage).reduce(Vector::plus).orElse(null);

            int cnt = nonNull.size();

            return new RMSPropParameterUpdate(squaresRunningAverageSum.divide(cnt), updatesSum.divide(cnt));
        } else
            return null;
    }

    /**
     * Get sum of updates.
     *
     * @param updates Updates.
     * @return Sum of updates.
     */
    public static RMSPropParameterUpdate sumLocal(List<RMSPropParameterUpdate> updates) {
        if (!updates.isEmpty()) {
            Vector updatesSum = updates.stream().map(RMSPropParameterUpdate::update).reduce(Vector::plus).orElse(null);

            // Last squares running average is the most actual.
            Vector lastSquaresRunningAverage = updates.get(updates.size() - 1).squaresRunningAverage();

            return new RMSPropParameterUpdate(lastSquaresRunningAverage, updatesSum);
        } else
            return null;
    }
}
