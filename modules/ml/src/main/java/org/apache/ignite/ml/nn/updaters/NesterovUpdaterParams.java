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
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

/**
 * Data needed for Nesterov parameters updater.
 */
public class NesterovUpdaterParams implements UpdaterParams<SmoothParametrized> {
    /**
     * Previous step weights updates.
     */
    protected Vector prevIterationUpdates;

    /**
     * Construct NesterovUpdaterParams.
     *
     * @param paramsCnt Count of parameters on which update happens.
     */
    public NesterovUpdaterParams(int paramsCnt) {
        prevIterationUpdates = new DenseLocalOnHeapVector(paramsCnt).assign(0);
    }

    /**
     * Set previous step parameters updates.
     *
     * @param updates Parameters updates.
     * @return This object with updated parameters updates.
     */
    public NesterovUpdaterParams setPreviousUpdates(Vector updates) {
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

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <M extends SmoothParametrized> M update(M obj) {
        Vector parameters = obj.parameters();
        return (M)obj.setParameters(parameters.minus(prevIterationUpdates));
    }
}
