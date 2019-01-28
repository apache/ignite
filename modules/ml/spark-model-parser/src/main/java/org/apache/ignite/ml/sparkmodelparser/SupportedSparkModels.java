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

package org.apache.ignite.ml.sparkmodelparser;

/**
 * List of supported Spark models.
 *
 * NOTE: Valid for Spark 2.4.
 */
public enum SupportedSparkModels {
    /** Logistic regression. */
    LOG_REGRESSION,

    /** Linear regression. */
    LINEAR_REGRESSION,

    /** Decision tree. */
    DECISION_TREE,

    /** Support Vector Machine . */
    LINEAR_SVM,

    /** Random forest. */
    RANDOM_FOREST,

    /**
     * Gradient boosted trees.
     * NOTE: support binary classification only with raw labels 0 and 1
     */
    GRADIENT_BOOSTED_TREES
}
