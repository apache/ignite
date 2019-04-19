/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.client.ClientException;

/**
 * Result of a function throwing an exception.
 */
final class Result<T> {
    /** Value. */
    private final T val;

    /** Exception. */
    private final ClientException ex;

    /**
     * Initializes a successful result.
     */
    Result(T val) {
        this.val = val;
        ex = null;
    }

    /**
     * Initializes a failed result.
     */
    Result(ClientException ex) {
        if (ex == null)
            throw new NullPointerException("ex");

        this.ex = ex;
        val = null;
    }

    /**
     * @return Value;
     */
    public T get() throws ClientException {
        if (ex != null)
            throw ex;

        return val;
    }
}
