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

package org.apache.ignite.ml.optimization;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.util.Utils;

/**
 * Base interface for parametrized models.
 *
 * @param <M> Model class.
 */
interface BaseParametrized<M extends BaseParametrized<M>> {
    /**
     * Get parameters vector.
     *
     * @return Parameters vector.
     */
    public Vector parameters();

    /**
     * Set parameters.
     *
     * @param vector Parameters vector.
     */
    public M setParameters(Vector vector);

    /**
     * Return new model with given parameters vector.
     *
     * @param vector Parameters vector.
     */
    public default M withParameters(Vector vector) {
        return Utils.copy(this).setParameters(vector);
    }

    /**
     * Get count of parameters of this model.
     *
     * @return Count of parameters of this model.
     */
    public int parametersCount();
}
