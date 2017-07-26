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

package org.apache.ignite.ml.trees;

import java.util.BitSet;

/**
 *
 */
public class CategoricalRegionInfo extends RegionInfo {
    /**
     * Indexes of vectors that belong to this region.
     **/
    private int[] vectors;

    /**
     * Bitset representing categories of this region.
     */
    private BitSet cats;

    /**
     * @param impurity Impurity of region.
     * @param vectors Indexes of vectors that belong to this region.
     * @param cats Bitset representing categories of this region.
     */
    public CategoricalRegionInfo(double impurity, int[] vectors, BitSet cats) {
        super(impurity);

        this.vectors = vectors;
        this.cats = cats;
    }

    /**
     * Get indexes of vectors that belong to this region.
     *
     * @return Indexes of vectors that belong to this region.
     */
    public int[] vectors() {
        return vectors;
    }

    /**
     * Get bitset representing categories of this region.
     *
     * @return Bitset representing categories of this region.
     */
    public BitSet cats() {
        return cats;
    }
}
