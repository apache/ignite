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

package org.apache.ignite.internal.util.lang;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;

/**
 * This exception provides closures with facility to throw exceptions. Closures can't
 * throw checked exception and this class provides a standard idiom on how to wrap and pass an
 * exception up the call chain.
 *
 * @see GridFunc#wrap(Throwable)
 */
public class GridClosureException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates wrapper closure exception for given {@link IgniteCheckedException}.
     *
     * @param e Exception to wrap.
     */
    public GridClosureException(Throwable e) {
        super(e);
    }

    /**
     * Unwraps the original {@link Throwable} instance.
     *
     * @return The original {@link Throwable} instance.
     */
    public Throwable unwrap() {
        return getCause();
    }
}