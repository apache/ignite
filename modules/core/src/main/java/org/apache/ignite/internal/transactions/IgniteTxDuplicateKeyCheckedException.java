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

package org.apache.ignite.internal.transactions;

import org.apache.ignite.IgniteCheckedException;

/**
 * Exception thrown whenever same entry version is inserted more than once within same transaction.
 */
public class IgniteTxDuplicateKeyCheckedException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new duplicate key exception with given error message.
     *
     * @param msg Error message.
     */
    public IgniteTxDuplicateKeyCheckedException(String msg) {
        super(msg);
    }

    /**
     * Creates new duplicate key exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be <tt>null</tt>).
     */
    public IgniteTxDuplicateKeyCheckedException(String msg, Throwable cause) {
        super(msg, cause);
    }
}