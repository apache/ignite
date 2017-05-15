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

package org.apache.ignite.ml.math.exceptions;

import org.apache.ignite.IgniteException;

/**
 * This exception is used to indicate error condition of matrix elements failing the positivity check.
 */
public class NonPositiveDefiniteMatrixException extends IgniteException {
    /**
     * Construct an exception.
     *
     * @param wrong Value that fails the positivity check.
     * @param idx Row (and column) index.
     * @param threshold Absolute positivity threshold.
     */
    public NonPositiveDefiniteMatrixException(double wrong, int idx, double threshold) {
        super("Matrix must be positive, wrong element located on diagonal with index "
            + idx + " and has value " + wrong + " with this threshold " + threshold);
    }
}
