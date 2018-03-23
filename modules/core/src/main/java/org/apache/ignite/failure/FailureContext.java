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
 * Failure context class, which instances handled by configured FailureHandler instance when Ignite failure occurs.
 */
public class FailureContext {
    /** Type. */
    private final FailureType type;

    /** Error. */
    private final Throwable error;

    /**
     * @param type Type.
     * @param error Cause.
     */
    public FailureContext(FailureType type, Throwable error) {
        assert type != null;

        this.type = type;
        this.error = error;
    }

    /**
     * @return FailureType value.
     */
    public FailureType type() {
        return type;
    }

    /**
     * @return error or {@code null}.
     */
    public Throwable error() {
        return error;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "FailureContext [" +
            "type=" + type +
            ", error=" + error +
            ']';
    }
}