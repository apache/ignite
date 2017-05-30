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

package org.apache.ignite.console.agent.rest;

/**
 * Request result.
 */
public class RestResult {
    /** REST http code. */
    private final int status;

    /** The field contains description of error if server could not handle the request. */
    private final String error;

    /** The field contains result of command. */
    private final String data;

    /**
     * @param status REST http code.
     * @param error The field contains description of error if server could not handle the request.
     * @param data The field contains result of command.
     */
    private RestResult(int status, String error, String data) {
        this.status = status;
        this.error = error;
        this.data = data;
    }

    /**
     * @param status REST http code.
     * @param error The field contains description of error if server could not handle the request.
     * @return Request result.
     */
    public static RestResult fail(int status, String error) {
        return new RestResult(status, error, null);
    }

    /**
     * @param data The field contains result of command.
     * @return Request result.
     */
    public static RestResult success(String data) {
        return new RestResult(0, null, data);
    }

    /**
     * @return REST http code.
     */
    public int getStatus() {
        return status;
    }

    /**
     * @return The field contains description of error if server could not handle the request.
     */
    public String getError() {
        return error;
    }

    /**
     * @return The field contains result of command.
     */
    public String getData() {
        return data;
    }
}
