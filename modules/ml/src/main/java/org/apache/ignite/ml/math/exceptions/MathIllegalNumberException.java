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
 * This class is based on the corresponding class from Apache Common Math lib.
 * Base class for exceptions raised by a wrong number.
 * This class is not intended to be instantiated directly: it should serve
 * as a base class to create all the exceptions that are raised because some
 * precondition is violated by a number argument.
 */
public class MathIllegalNumberException extends MathIllegalArgumentException {
    /** Serializable version Id. */
    private static final long serialVersionUID = -7447085893598031110L;

    /** Requested. */
    private final Number arg;

    /**
     * Construct an exception.
     *
     * @param msg Localizable pattern.
     * @param wrong Wrong number.
     * @param arguments Arguments.
     */
    protected MathIllegalNumberException(String msg, Number wrong, Object... arguments) {
        super(msg, wrong, arguments);
        arg = wrong;
    }

    /**
     * @return the requested value.
     */
    public Number getArg() {
        return arg;
    }
}
