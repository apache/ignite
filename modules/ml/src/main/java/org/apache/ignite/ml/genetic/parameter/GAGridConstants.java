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

package org.apache.ignite.ml.genetic.parameter;

/**
 * GAGridConstants
 */
public interface GAGridConstants {
    /** populationCache constant */
    public static final String POPULATION_CACHE = "populationCache";

    /** populationCache constant */
    public static final String GENE_CACHE = "geneCache";

    /** Selection Method type **/
    public enum SELECTION_METHOD {
        /** Selecton method eletism. */
        SELECTON_METHOD_ELETISM,
        /** Selection method truncation. */
        SELECTION_METHOD_TRUNCATION,
        /** Selection method roulette wheel. */
        SELECTION_METHOD_ROULETTE_WHEEL
    }
}
