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

package org.apache.ignite.rest;

/**
 * Error result represent a tuple of error type and user-friendly error message.
 */
public class ErrorResult {
    /** Error type describing the class of the error occurred. */
    private final String type;

    /** User-friendly error message. */
    private final String message;

    /**
     * Constructor.
     *
     * @param type    Error type describing the class of the error occurred.
     * @param message User-friendly error message.
     */
    public ErrorResult(String type, String message) {
        this.type = type;
        this.message = message;
    }

    /**
     * Returns error type describing the class of the error occurred.
     *
     * @return Error type describing the class of the error occurred.
     */
    public String type() {
        return type;
    }

    /**
     * Returns user-friendly error message.
     *
     * @return User-friendly error message.
     */
    public String message() {
        return message;
    }
}
