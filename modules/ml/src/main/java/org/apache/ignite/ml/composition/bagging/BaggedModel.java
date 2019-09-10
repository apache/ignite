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

package org.apache.ignite.ml.composition.bagging;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * This class represents model produced by {@link BaggedTrainer}.
 * It is a wrapper around inner representation of model produced by {@link BaggedTrainer}.
 */
public final class BaggedModel implements IgniteModel<Vector, Double>, DeployableObject {
    /** Inner representation of model produced by {@link BaggedTrainer}. */
    private IgniteModel<Vector, Double> mdl;

    /**
     * Construct instance of this class given specified model.
     * @param mdl Model to wrap.
     */
    BaggedModel(IgniteModel<Vector, Double> mdl) {
        this.mdl = mdl;
    }

    /**
     * Get wrapped model.
     *
     * @return Wrapped model.
     */
    IgniteModel<Vector, Double> model() {
        return mdl;
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector i) {
        return mdl.predict(i);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        mdl.close();
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.singletonList(mdl);
    }
}
