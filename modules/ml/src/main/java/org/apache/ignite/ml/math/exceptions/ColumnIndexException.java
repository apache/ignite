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

package org.apache.ignite.ml.math.exceptions;

import org.apache.ignite.IgniteException;

/**
 * This exception is used to indicate any error condition accessing matrix elements by invalid column index.
 */
public class ColumnIndexException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param idx Index value that caused this exception.
     */
    public ColumnIndexException(int idx) {
        super("Invalid (out of bound) column index: " + idx);
    }
}
