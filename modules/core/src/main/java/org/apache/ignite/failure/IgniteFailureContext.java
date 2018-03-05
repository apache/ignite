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
 * Failure Context.
 */
public class IgniteFailureContext {
    /** Type. */
    private final IgniteFailureType type;

    /** Cause. */
    private final Throwable cause;

    /**
     * @param type Type.
     * @param cause Cause.
     */
    public IgniteFailureContext(IgniteFailureType type, Throwable cause) {
        assert type != null;

        this.type = type;
        this.cause = cause;
    }

    /**
     * @return IgniteFailureType value.
     */
    public IgniteFailureType type() {
        return type;
    }

    /**
     * @return cause or {@code null}.
     */
    public Throwable cause() {
        return cause;
    }

    @Override public String toString() {
        return "IgniteFailureContext{" +
            "type=" + type +
            ", cause=" + cause +
            '}';
    }
}