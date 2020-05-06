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
 *
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.IgniteException;

/**
 * Thrown when {@link IgniteHistoricalIterator} cannot iterate over WAL for some reason.
 */
public class IgniteHistoricalIteratorException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a new exception with the specified cause.
     *
     * @param cause Cause.
     */
    public IgniteHistoricalIteratorException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new exception with the specified message and cause.
     *
     * @param msg Detail message.
     * @param cause Cause.
     */
    public IgniteHistoricalIteratorException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
