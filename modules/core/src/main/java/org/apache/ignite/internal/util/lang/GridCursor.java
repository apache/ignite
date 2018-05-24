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

/**
 * Simple cursor abstraction. Initial state must be "before first".
 */
public interface GridCursor<T> {
    /**
     * Attempt to move cursor position forward.
     *
     * @return {@code true} If we were able to move position of cursor forward.
     * @throws IgniteCheckedException If failed.
     */
    public boolean next() throws IgniteCheckedException;

    /**
     * Gets element at current position. Must be called only after successful {@link #next()} call.
     *
     * @return Element at current position.
     * @throws IgniteCheckedException If failed.
     */
    public T get() throws IgniteCheckedException;
}
