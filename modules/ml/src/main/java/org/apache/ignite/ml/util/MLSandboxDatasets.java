/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.util;

/**
 * The names of popular datasets used in examples.
 */
public enum MLSandboxDatasets {
    /** Movielens dataset with ratings. */
    MOVIELENS("ratings.csv", true, ","),

    /** The full Iris dataset from Machine Learning Repository. */
    IRIS("iris.txt", false, "\t"),

    /** The Titanic dataset from Kaggle competition. */
    TITANIC("titanic.csv", true, ";"),

    /** The 1st and 2nd classes from the Iris dataset. */
    TWO_CLASSED_IRIS("two_classed_iris.csv", false, "\t"),

    /** The dataset is about different computers' properties based on https://archive.ics.uci.edu/ml/datasets/Computer+Hardware. */
    CLEARED_MACHINES("cleared_machines.csv", false, ";"),

    /**
     * The health data is related to death rate based on; doctor availability, hospital availability,
     * annual per capita income, and population density people per square mile.
     */
    MORTALITY_DATA("mortalitydata.csv", false, ";"),

    /**
     * The preprocessed Glass dataset from the Machine Learning Repository https://archive.ics.uci.edu/ml/datasets/Glass+Identification
     * There are 3 classes with labels: 1 {building_windows_float_processed}, 3 {vehicle_windows_float_processed}, 7 {headlamps}.
     * Feature names: 'Na-Sodium', 'Mg-Magnesium', 'Al-Aluminum', 'Ba-Barium', 'Fe-Iron'.
     */
    GLASS_IDENTIFICATION("glass_identification.csv", false, ";"),

    /** The Wine recognition data. Could be found <a href="https://archive.ics.uci.edu/ml/machine-learning-databases/wine/">here</a>. */
    WINE_RECOGNITION("wine.txt", false, ","),

    /** The Boston house-prices dataset. Could be found <a href="https://archive.ics.uci.edu/ml/machine-learning-databases/housing/">here</a>. */
    BOSTON_HOUSE_PRICES("boston_housing_dataset.txt", false, ","),

    /** Example from book Barber D. Bayesian reasoning and machine learning. Chapter 10. */
    ENGLISH_VS_SCOTTISH("english_vs_scottish_binary_dataset.csv", true, ","),

    /** Wholesale customers dataset. Could be found <a href="https://archive.ics.uci.edu/ml/datasets/Wholesale+customers">here</a>. */
    WHOLESALE_CUSTOMERS("wholesale_customers.csv", true, ","),

    /** Fraud detection problem [part of whole dataset]. Could be found <a href="https://www.kaggle.com/mlg-ulb/creditcardfraud/">here</a>. */
    FRAUD_DETECTION("fraud_detection.csv", false, ",");

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
