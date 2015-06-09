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

package org.apache.ignite.internal.visor.util;

/**
 * Exception wrapper for safe for transferring to Visor.
 */
public class VisorExceptionWrapper extends Throwable {
    /** Detail message string of this throwable */
    private String detailMsg;

    /** Simple class name of base throwable object. */
    private String classSimpleName;

    /** Class name of base throwable object. */
    private String className;

    /**
     * Wrap throwable by presented on Visor throwable object.
     *
     * @param cause Base throwable object.
     */
    public VisorExceptionWrapper(Throwable cause) {
        assert cause != null;

        classSimpleName = cause.getClass().getSimpleName();
        className = cause.getClass().getName();

        detailMsg = cause.getMessage();

        StackTraceElement[] stackTrace = cause.getStackTrace();

        if (stackTrace != null)
            setStackTrace(stackTrace);

        if (cause.getCause() != null)
            initCause(new VisorExceptionWrapper(cause.getCause()));
    }

    /**
     * @return Class simple name of base throwable object.
     */
    public String getClassSimpleName() {
        return classSimpleName;
    }

    /**
     * @return Class name of base throwable object.
     */
    public String getClassName() {
        return className;
    }

    /** {@inheritDoc} */
    @Override public String getMessage() {
        return detailMsg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return (detailMsg != null) ? (className + ": " + detailMsg) : className;
    }
}
