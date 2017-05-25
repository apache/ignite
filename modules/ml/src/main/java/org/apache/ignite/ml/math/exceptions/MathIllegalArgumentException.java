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
 * Base class for all preconditions violation exceptions.
 * In most cases, this class should not be instantiated directly: it should
 * serve as a base class to create all the exceptions that have the semantics
 * of the standard {@link IllegalArgumentException}.
 */
public class MathIllegalArgumentException extends MathRuntimeException {
    /** Serializable version Id. */
    private static final long serialVersionUID = -6024911025449780478L;

    /**
     * @param format Message format string explaining the cause of the error.
     * @param args Arguments.
     */
    public MathIllegalArgumentException(String format, Object... args) {
        super(String.format(format, args));
    }

}
