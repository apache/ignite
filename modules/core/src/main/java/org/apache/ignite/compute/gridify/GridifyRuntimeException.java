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

package org.apache.ignite.compute.gridify;

import org.apache.ignite.IgniteException;

/**
 * This defines gridify exception. This runtime exception gets thrown out of gridified
 * methods in case if method execution resulted in undeclared exception.
 */
public class GridifyRuntimeException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new gridify runtime exception with specified message.
     *
     * @param msg Exception message.
     */
    public GridifyRuntimeException(String msg) {
        super(msg);
    }

    /**
     * Creates new gridify runtime exception given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridifyRuntimeException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates new gridify runtime exception with specified message and cause.
     *
     * @param msg Exception message.
     * @param cause Exception cause.
     */
    public GridifyRuntimeException(String msg, Throwable cause) {
        super(msg, cause);
    }
}