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
