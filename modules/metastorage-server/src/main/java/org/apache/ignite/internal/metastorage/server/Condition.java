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

package org.apache.ignite.internal.metastorage.server;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * Defines interface for condition which could be applied to an entry.
 * 
 * @see KeyValueStorage#invoke(Condition, Collection, Collection) 
 */
public interface Condition {
    /**
     * Returns the key which identifies an entry which condition will applied to.
     *
     * @return The key which identifies an entry which condition will applied to.
     */
    @NotNull byte[] key();

    /**
     * Tests the given entry on condition.
     *
     * @param e Entry which will be tested on the condition. Can't be {@code null}.
     * @return {@code True} if the given entry satisfies to the condition, otherwise - {@code false}.
     */
    boolean test(@NotNull Entry e);
}
