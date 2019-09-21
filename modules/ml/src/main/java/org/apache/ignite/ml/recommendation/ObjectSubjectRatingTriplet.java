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

/**
 * Object-subject-rating triplet.
 *
 * @param <O> Type of an object.
 * @param <S> Type of a subject.
 */
public class ObjectSubjectRatingTriplet<O extends Serializable, S extends Serializable>
    extends ObjectSubjectPair<O, S> {
    /** */
    private static final long serialVersionUID = 2983086891476351865L;

    /** Rating. */
    private final Double rating;

    /**
     * Constructs a new instance of object-subject-rating triplet.
     *
     * @param obj Object.
     * @param subj Subject.
     * @param rating Rating.
     */
    public ObjectSubjectRatingTriplet(O obj, S subj, Double rating) {
        super(obj, subj);
        this.rating = rating;
    }

    /**
     * Returns rating.
     *
     * @return Rating.
     */
    public Double getRating() {
        return rating;
    }
}
