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
package org.apache.ignite.ml.math.exceptions.math;

import org.apache.ignite.IgniteException;

/**
 * This class is based on the corresponding class from Apache Common Math lib.
 * In most cases, this class should not be instantiated directly: it should
 * serve as a base class for implementing exception classes that describe a
 * specific "problem".
 */
public class MathRuntimeException extends IgniteException {
    /** Serializable version Id. */
    private static final long serialVersionUID = 20120926L;

    /**
     * @param format Message pattern explaining the cause of the error.
     * @param args Arguments.
     */
    public MathRuntimeException(String format, Object... args) {
        this(null, format, args);
    }

    /**
     * @param cause Root cause.
     * @param format Message pattern explaining the cause of the error.
     * @param args Arguments.
     */
    public MathRuntimeException(Throwable cause, String format, Object... args) {
        super(String.format(format, args), cause);
    }
}
