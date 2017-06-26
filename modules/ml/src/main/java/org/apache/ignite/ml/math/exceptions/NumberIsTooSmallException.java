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

/**
 * Exception to be thrown when a number is too small.
 */
public class NumberIsTooSmallException extends MathIllegalNumberException {
    /** */
    private static final String NUMBER_TOO_SMALL = "Number {1} is smaller than the minimum ({2}).";

    /** */
    private static final String NUMBER_TOO_SMALL_BOUND_EXCLUDED = "Number {1} is smaller than, or equal to, the minimum ({2}).";

    /** Serializable version Id. */
    private static final long serialVersionUID = -6100997100383932834L;
    /**
     * Higher bound.
     */
    private final Number min;
    /**
     * Whether the maximum is included in the allowed range.
     */
    private final boolean boundIsAllowed;

    /**
     * Construct the exception.
     *
     * @param wrong Value that is smaller than the minimum.
     * @param min Minimum.
     * @param boundIsAllowed Whether {@code min} is included in the allowed range.
     */
    public NumberIsTooSmallException(Number wrong, Number min, boolean boundIsAllowed) {
        this(boundIsAllowed ? NUMBER_TOO_SMALL : NUMBER_TOO_SMALL_BOUND_EXCLUDED,
            wrong, min, boundIsAllowed);
    }

    /**
     * Construct the exception with a specific context.
     *
     * @param msg Specific context pattern.
     * @param wrong Value that is smaller than the minimum.
     * @param min Minimum.
     * @param boundIsAllowed Whether {@code min} is included in the allowed range.
     */
    public NumberIsTooSmallException(String msg, Number wrong, Number min, boolean boundIsAllowed) {
        super(msg, wrong, min);
        this.min = min;
        this.boundIsAllowed = boundIsAllowed;
    }

    /**
     * @return {@code true} if the minimum is included in the allowed range.
     */
    public boolean getBoundIsAllowed() {
        return boundIsAllowed;
    }

    /**
     * @return the minimum.
     */
    public Number getMin() {
        return min;
    }
}
