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
    /** The full Iris dataset from Machine Learning Repository. */
    IRIS("datasets/iris.txt", false, "\t"),

    /** The Titanic dataset from Kaggle competition. */
    TITANIC("datasets/titanic.csv", true, ";"),

    /** The 1st and 2nd classes from the Iris dataset. */
    TWO_CLASSED_IRIS("datasets/two_classed_iris.csv", false, "\t"),

    /** The dataset is about different computers' properties based on https://archive.ics.uci.edu/ml/datasets/Computer+Hardware. */
    CLEARED_MACHINES("datasets/cleared_machines.csv", false, ";"),

    /**
     * The health data is related to death rate based on; doctor availability, hospital availability,
     * annual per capita income, and population density people per square mile.
     */
    MORTALITY_DATA("datasets/mortalitydata.csv", false, ";"),

    /**
     * The preprocessed Glass dataset from the Machine Learning Repository https://archive.ics.uci.edu/ml/datasets/Glass+Identification
     * There are 3 classes with labels: 1 {building_windows_float_processed}, 3 {vehicle_windows_float_processed}, 7 {headlamps}.
     * Feature names: 'Na-Sodium', 'Mg-Magnesium', 'Al-Aluminum', 'Ba-Barium', 'Fe-Iron'.
     */
    GLASS_IDENTIFICATION("datasets/glass_identification.csv", false, ";"),

    /** The Wine recognition data. Could be found <a href="https://archive.ics.uci.edu/ml/machine-learning-databases/wine/">here</a>. */
    WINE_RECOGNITION("datasets/wine.txt", false, ","),

    /** The Boston house-prices dataset. Could be found <a href="https://archive.ics.uci.edu/ml/machine-learning-databases/housing/">here</a>. */
    BOSTON_HOUSE_PRICES("datasets/boston_housing_dataset.txt", false, ",");

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
