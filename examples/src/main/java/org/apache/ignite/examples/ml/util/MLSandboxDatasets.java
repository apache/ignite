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

package org.apache.ignite.examples.ml.util;

/**
 * The names of popular datasets used in examples.
 */
public enum MLSandboxDatasets {
    /** Movielens dataset with ratings. */
    MOVIELENS("examples/src/main/resources/datasets/ratings.csv", true, ","),

    /** The full Iris dataset from Machine Learning Repository. */
    IRIS("examples/src/main/resources/datasets/iris.txt", false, "\t"),

    /** The Titanic dataset from Kaggle competition. */
    TITANIC("examples/src/main/resources/datasets/titanic.csv", true, ";"),

    /** The 1st and 2nd classes from the Iris dataset. */
    TWO_CLASSED_IRIS("examples/src/main/resources/datasets/two_classed_iris.csv", false, "\t"),

    /** The dataset is about different computers' properties based on https://archive.ics.uci.edu/ml/datasets/Computer+Hardware. */
    CLEARED_MACHINES("examples/src/main/resources/datasets/cleared_machines.csv", false, ";"),

    /**
     * The health data is related to death rate based on; doctor availability, hospital availability,
     * annual per capita income, and population density people per square mile.
     */
    MORTALITY_DATA("examples/src/main/resources/datasets/mortalitydata.csv", false, ";"),

    /**
     * The preprocessed Glass dataset from the Machine Learning Repository https://archive.ics.uci.edu/ml/datasets/Glass+Identification
     * There are 3 classes with labels: 1 {building_windows_float_processed}, 3 {vehicle_windows_float_processed}, 7 {headlamps}.
     * Feature names: 'Na-Sodium', 'Mg-Magnesium', 'Al-Aluminum', 'Ba-Barium', 'Fe-Iron'.
     */
    GLASS_IDENTIFICATION("examples/src/main/resources/datasets/glass_identification.csv", false, ";"),

    /** The Wine recognition data. Could be found <a href="https://archive.ics.uci.edu/ml/machine-learning-databases/wine/">here</a>. */
    WINE_RECOGNITION("examples/src/main/resources/datasets/wine.txt", false, ","),

    /** The Boston house-prices dataset. Could be found <a href="https://archive.ics.uci.edu/ml/machine-learning-databases/housing/">here</a>. */
    BOSTON_HOUSE_PRICES("examples/src/main/resources/datasets/boston_housing_dataset.txt", false, ","),

    /** Example from book Barber D. Bayesian reasoning and machine learning. Chapter 10. */
    ENGLISH_VS_SCOTTISH("examples/src/main/resources/datasets/english_vs_scottish_binary_dataset.csv", true, ","),

    /** Wholesale customers dataset. Could be found <a href="https://archive.ics.uci.edu/ml/datasets/Wholesale+customers">here</a>. */
    WHOLESALE_CUSTOMERS("examples/src/main/resources/datasets/wholesale_customers.csv", true, ","),

    /** Fraud detection problem [part of whole dataset]. Could be found <a href="https://www.kaggle.com/mlg-ulb/creditcardfraud/">here</a>. */
    FRAUD_DETECTION("examples/src/main/resources/datasets/fraud_detection.csv", false, ","),

    /** A dataset with discrete and continuous features. */
    MIXED_DATASET("examples/src/main/resources/datasets/mixed_dataset.csv", true, ","),

    /** A dataset with categorical features and labels. */
    MUSHROOMS("examples/src/main/resources/datasets/mushrooms.csv", true, ","),

    /** A dataset with categorical features and labels. */
    AMAZON_EMPLOYEE_ACCESS("examples/src/main/resources/datasets/amazon-employee-access-challenge_train.csv", true, ",");

    /** Filename. */
    private final String filename;

    /** The csv file has header. */
    private final boolean hasHeader;

    /** The separator between words. */
    private final String separator;

    /**
     * @param filename Filename.
     * @param hasHeader The csv file has header.
     * @param separator The special sign to separate the line on words.
     */
    MLSandboxDatasets(final String filename, boolean hasHeader, String separator) {
        this.filename = filename;
        this.hasHeader = hasHeader;
        this.separator = separator;
    }

    /** */
    public String getFileName() { return filename; }

    /** */
    public boolean hasHeader() {
        return hasHeader;
    }

    /** */
    public String getSeparator() {
        return separator;
    }
}
