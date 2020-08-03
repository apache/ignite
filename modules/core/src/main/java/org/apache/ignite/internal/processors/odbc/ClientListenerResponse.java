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

package org.apache.ignite.internal.processors.odbc;

import org.jetbrains.annotations.Nullable;

/**
 * Client listener response.
 */
public abstract class ClientListenerResponse {
    /** Command succeeded. */
    public static final int STATUS_SUCCESS = 0;

    /** Command failed. */
    public static final int STATUS_FAILED = 1;

    /** Success status. */
    private int status;

    /** Error. */
    private String err;

    /**
     * Constructs failed rest response.
     *
     * @param status Response status.
     * @param err Error, {@code null} if success is {@code true}.
     */
    protected ClientListenerResponse(int status, @Nullable String err) {
        this.status = status;
        this.err = err;
    }

    /**
     * @return Success flag.
     */
    public int status() {
        return status;
    }

    /**
     * @param status Status.
     */
    public void status(int status) {
        this.status = status;
    }

    /**
     * @return Error.
     */
    public String error() {
        return err;
    }

    /**
     * @param err Error message.
     */
    public void error(String err) {
        this.err = err;
    }

    /**
     * Callback for response sent event.
     */
    public void onSent() {
        // No-op.
    }
}
