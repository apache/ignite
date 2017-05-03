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

package org.apache.ignite.ml.regressions;

/**
 * This class contains various messages used in regressions,
 */
public class RegressionsErrorMessages {
    /** Constant for string indicating that sample has insufficient observed points. */
    static final String INSUFFICIENT_OBSERVED_POINTS_IN_SAMPLE = "Insufficient observed points in sample.";
    /** */
    static final String NOT_ENOUGH_DATA_FOR_NUMBER_OF_PREDICTORS = "Not enough data (%d rows) for this many predictors (%d predictors)";
}
