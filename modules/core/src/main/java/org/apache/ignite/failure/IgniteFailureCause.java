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

package org.apache.ignite.failure;

/**
 *
 */
public class IgniteFailureCause {
    /** Type. */
    private final Type type;

    /** Cause. */
    private final Throwable cause;

    /**
     * @param type Type.
     * @param cause Cause.
     */
    public IgniteFailureCause(Type type, Throwable cause) {
        assert type != null;

        this.type = type;
        this.cause = cause;
    }

    /**
     *
     */
    public Type type() {
        return type;
    }

    /**
     *
     */
    public Throwable cause() {
        return cause;
    }

    /**
     * Enumeration of IgniteFailureCause types.
     */
    public enum Type {
        /** Exchange worker stop. */
        EXCHANGE_WORKER_STOP,

        /** Persistence error. */
        PERSISTENCE_ERROR,

        /** Ignite out of memory error. */
        IGNITE_OUT_OF_MEMORY_ERROR;
    }
}
