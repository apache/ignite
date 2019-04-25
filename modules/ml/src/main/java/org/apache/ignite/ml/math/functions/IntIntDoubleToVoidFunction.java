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

package org.apache.ignite.ml.math.functions;

import java.io.Serializable;

/**
 * Setter function for matrices.
 */
public interface IntIntDoubleToVoidFunction extends Serializable {
    /**
     * @param x Row index in the matrix.
     * @param y Column index in the matrix.
     * @param v Value to set at given row and column.
     */
    public void apply(int x, int y, double v);
}
