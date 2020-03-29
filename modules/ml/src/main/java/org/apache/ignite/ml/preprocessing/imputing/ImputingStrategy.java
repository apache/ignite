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

package org.apache.ignite.ml.preprocessing.imputing;

/** This enum contains settings for imputing preprocessor. */
public enum ImputingStrategy {
    /** The default strategy. If this strategy is chosen, then replace missing values using the mean for the numeric features along the axis. */
    MEAN,

    /** If this strategy is chosen, then replace missing using the most frequent value along the axis. */
    MOST_FREQUENT,

    /** If this strategy is chosen, then replace missing using the minimal value along the axis. */
    MIN,

    /** If this strategy is chosen, then replace missing using the maximum value along the axis. */
    MAX,

    /** If this strategy is chosen, then replace missing using the amount of values along the axis. */
    COUNT,

    /** If this strategy is chosen, then replace missing using the least frequent value along the axis. */
    LEAST_FREQUENT
}
