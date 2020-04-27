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
 * It doesn't support old models from MLLib package.
 *
 * NOTE: Valid for Spark 2.2-2.4.
 */
public enum SupportedSparkModels {
    /** Logistic regression. */
    LOG_REGRESSION("org.apache.spark.ml.classification.LogisticRegressionModel"),

    /** Linear regression. */
    LINEAR_REGRESSION("org.apache.spark.ml.regression.LinearRegressionModel"),

    /** Decision tree. */
    DECISION_TREE("org.apache.spark.ml.classification.DecisionTreeClassificationModel"),

    /** Support Vector Machine . */
    LINEAR_SVM("org.apache.spark.ml.classification.LinearSVCModel"),

    /** Random forest. */
    RANDOM_FOREST("org.apache.spark.ml.classification.RandomForestClassificationModel"),

    /** K-Means. */
    KMEANS("org.apache.spark.ml.clustering.KMeansModel"),

    /** Decision tree regression. */
    DECISION_TREE_REGRESSION("org.apache.spark.ml.regression.DecisionTreeRegressionModel"),

    /** Random forest regression. */
    RANDOM_FOREST_REGRESSION("org.apache.spark.ml.regression.RandomForestRegressionModel"),

    /** Gradient boosted trees regression. */
    GRADIENT_BOOSTED_TREES_REGRESSION("org.apache.spark.ml.regression.GBTRegressionModel"),

    /**
     * Gradient boosted trees.
     * NOTE: support binary classification only with raw labels 0 and 1
     */
    GRADIENT_BOOSTED_TREES("org.apache.spark.ml.classification.GBTClassificationModel");

    /** The separator between words. */
    private final String mdlClsNameInSpark;

    /**
     * @param mdlClsNameInSpark Model class name in Apache Spark.
     */
    SupportedSparkModels(String mdlClsNameInSpark) {
        this.mdlClsNameInSpark = mdlClsNameInSpark;
    }

    /** Get model class name in Apache Spark. */
    public String getMdlClsNameInSpark() {
        return mdlClsNameInSpark;
    }
}
