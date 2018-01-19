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

package org.apache.ignite.ml.trainers.group;

import org.apache.ignite.ml.Model;

/**
 * Model which outputs given constant.
 *
 * @param <T> Type of constant.
 */
public class ConstModel<T> implements Model<T, T> {
    /**
     * Constant to be returned by this model.
     */
    private T c;

    /**
     * Create instance of this class specified by input parameters.
     *
     * @param c Constant to be returned by this model.
     */
    public ConstModel(T c) {
        this.c = c;
    }

    /** {@inheritDoc} */
    @Override public T apply(T val) {
        return c;
    }
}
