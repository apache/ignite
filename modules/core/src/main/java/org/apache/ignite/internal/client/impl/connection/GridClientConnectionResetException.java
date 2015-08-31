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

package org.apache.ignite.internal.client.impl.connection;

import org.apache.ignite.internal.client.GridClientException;

/**
 * This exception is thrown when ongoing packet should be sent, but network connection is broken.
 * In this case client will try to reconnect to any of the servers specified in configuration.
 */
public class GridClientConnectionResetException extends GridClientException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates an exception with given message.
     *
     * @param msg Error message.
     */
    GridClientConnectionResetException(String msg) {
        super(msg);
    }

    /**
     * Creates an exception with given message and error cause.
     *
     * @param msg Error message.
     * @param cause Wrapped exception.
     */
    GridClientConnectionResetException(String msg, Throwable cause) {
        super(msg, cause);
    }
}