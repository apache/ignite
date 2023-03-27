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
 * Object-subject pair.
 *
 * @param <O> Type of an object of recommendation.
 * @param <S> Type of a subject of recommendation.
 */
public class ObjectSubjectPair<O extends Serializable, S extends Serializable> implements Serializable {
    /** */
    private static final long serialVersionUID = 2016407650288003508L;

    /** Object of recommendation. */
    private final O obj;

    /** Subject of recommendation. */
    private final S subj;

    /**
     * Constructs a new instance of object-subject pair.
     *
     * @param obj Object of recommendation.
     * @param subj Subject of recommendation.
     */
    public ObjectSubjectPair(O obj, S subj) {
        this.obj = obj;
        this.subj = subj;
    }

    /**
     * Returns object of recommendation.
     *
     * @return Object of recommendation.
     */
    public O getObj() {
        return obj;
    }

    /**
     * Returns subject of recommendation.
     *
     * @return Subject of recommendation.
     */
    public S getSubj() {
        return subj;
    }
}
