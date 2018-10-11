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

package org.apache.ignite.client;

/**
 * Indicates all the Ignite servers specified in the client configuration are no longer available.
 */
public class ClientConnectionException extends ClientException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Message. */
    private static final String MSG = "Ignite cluster is unavailable";

    /**
     * Default constructor.
     */
    public ClientConnectionException() {
        super(MSG);
    }

    /**
     * Constructs a new exception with the specified cause.
     *
     * @param cause the cause.
     */
    public ClientConnectionException(Throwable cause) {
        super(MSG, cause);
    }
}
