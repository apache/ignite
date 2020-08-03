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

import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Failure context contains information about failure type and exception if applicable.
 * This information could be used for appropriate handling of the failure.
 */
public class FailureContext {
    /** Type. */
    private final FailureType type;

    /** Error. */
    private final Throwable err;

    /**
     * Creates instance of {@link FailureContext}.
     *
     * @param type Failure type.
     * @param err Exception.
     */
    public FailureContext(FailureType type, Throwable err) {
        A.notNull(type, "type");

        this.type = type;
        this.err = err;
    }

    /**
     * @return Failure type.
     */
    public FailureType type() {
        return type;
    }

    /**
     * @return Exception or {@code null}.
     */
    public Throwable error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FailureContext.class, this);
    }
}
