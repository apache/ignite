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

package org.apache.ignite.internal.processors.platform.client;

import java.sql.SQLException;
import org.apache.ignite.IgniteException;

/**
 * Client exception.
 */
public class IgniteClientException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Code to return as {@link SQLException#vendorCode} */
    private final int statusCode;

    /**
     * Constructor.
     *
     * @param statusCode Status code (see {@link ClientStatus}).
     * @param msg Message.
     */
    public IgniteClientException(int statusCode, String msg) {
        this(statusCode, msg, null);
    }

    /**
     * Constructor.
     *
     * @param statusCode Status code (see {@link ClientStatus}).
     * @param msg Message.
     * @param cause Cause.
     */
    public IgniteClientException(int statusCode, String msg, Exception cause) {
        super(msg, cause);

        this.statusCode = statusCode;
    }

    /**
     * Gets the status code.
     *
     * @return Status code.
     */
    public int statusCode() {
        return statusCode;
    }
}
