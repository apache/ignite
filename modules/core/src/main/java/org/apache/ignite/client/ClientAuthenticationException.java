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
 * Indicates Ignite server the client is connected to closed the connection and no longer available.
 */
public class ClientAuthenticationException extends ClientException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Message. */
    private static final String MSG = "Invalid user name or password";

    /**
     * Default constructor.
     */
    public ClientAuthenticationException() {
        super(MSG);
    }

    /**
     * Constructs a new exception with the specified cause.
     *
     * @param cause the cause.
     */
    public ClientAuthenticationException(Throwable cause) {
        super(MSG, cause);
    }
}
