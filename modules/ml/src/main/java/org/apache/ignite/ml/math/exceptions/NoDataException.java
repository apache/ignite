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

/**
 * This class is based on the corresponding class from Apache Common Math lib.
 * Exception to be thrown when the required data is missing.
 */
public class NoDataException extends MathIllegalArgumentException {
    /** Serializable version Id. */
    private static final long serialVersionUID = -3629324471511904459L;

    /** */
    private static final String NO_DATA = "No data.";

    /**
     * Construct the exception.
     */
    public NoDataException() {
        this(NO_DATA);
    }

    /**
     * Construct the exception with a specific message.
     *
     * @param msg Message.
     */
    public NoDataException(String msg) {
        super(msg);
    }
}
