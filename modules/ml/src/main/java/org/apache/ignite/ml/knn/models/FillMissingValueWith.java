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

package org.apache.ignite.ml.knn.models;

/** This enum describes different approaches to work with missed values */
public enum FillMissingValueWith {
    /**
     * Fill missed value with zero or empty string or default value for categorical features
     */
    ZERO,
    /**
     * Fill missed value with mean on column
     * Requires an additional time to calculate
     */
    MEAN,
    /**
     * Fill missed value with mode on column
     * Requires an additional time to calculate
     */
    MODE,
    /**
     * Deletes observation with missed values
     * Transforms dataset and changes indexing
     */
    DELETE
}
