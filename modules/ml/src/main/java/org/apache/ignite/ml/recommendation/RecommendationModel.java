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

package org.apache.ignite.ml.recommendation;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Recommendation model that predicts rating for {@link ObjectSubjectPair}.
 *
 * @param <O> Type of an object of recommendation.
 * @param <S> Type of a subject of recommendation.
 */
public class RecommendationModel<O extends Serializable, S extends Serializable>
    implements IgniteModel<ObjectSubjectPair<O, S>, Double> {
    /** */
    private static final long serialVersionUID = -3664382168079054785L;

    /** Object of recommendation matrix (result of factorization of rating matrix). */
    private final Map<O, Vector> objMatrix;

    /** Subject of recommendation matrix (result of factorization of rating matrix). */
    private final Map<S, Vector> subjMatrix;

    /**
     * Constructs a new instance of recommendation model.
     *
     * @param objMatrix Object of recommendation matrix.
     * @param subjMatrix Subject of recommendation matrix.
     */
    public RecommendationModel(Map<O, Vector> objMatrix, Map<S, Vector> subjMatrix) {
        this.objMatrix = Collections.unmodifiableMap(objMatrix);
        this.subjMatrix = Collections.unmodifiableMap(subjMatrix);
    }

    /** {@inheritDoc} */
    @Override public Double predict(ObjectSubjectPair<O, S> input) {
        Vector objVector = objMatrix.get(input.getObj());
        Vector subjVector = subjMatrix.get(input.getSubj());
        return objVector.dot(subjVector);
    }

    /** */
    public Map<O, Vector> getObjMatrix() {
        return objMatrix;
    }

    /** */
    public Map<S, Vector> getSubjMatrix() {
        return subjMatrix;
    }
}
