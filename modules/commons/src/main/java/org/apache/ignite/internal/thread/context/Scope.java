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

package org.apache.ignite.internal.thread.context;

/**
 * Represents the Scope of {@link OperationContext} attributes update. Explicitly calling {@link #close()} method undoes
 * the applied changes and restores previous attribute values, if any. Note that every Scope relating to a specific
 * {@link OperationContext} update must be closed to free up thread-bound resources and avoid memory leaks, so it is
 * highly encouraged to use a try-with-resource block with Scope instances.
 * <p>
 * Scope is result of the following {@link OperationContext} update operations:
 * <ul>
 *     <li>{@link OperationContext#set(OperationContextAttribute, Object)} - creates a new or update an existing mapping
 *     between specified {@link OperationContextAttribute} and its value</li>
 *     <li>{@link OperationContext#restoreSnapshot(OperationContextSnapshot)} - updates {@link OperationContextAttribute}
 *     values to match the values stored in {@link OperationContextSnapshot}</li>
 * </ul>
 * </p>
 *
 * @see OperationContext#set(OperationContextAttribute, Object)
 * @see OperationContext#restoreSnapshot(OperationContextSnapshot)
 */
public interface Scope extends AutoCloseable {
    /** Scope instance that does nothing when closed. */
    Scope NOOP_SCOPE = () -> {};

    /** Closes the scope. This operation cannot fail. */
    @Override void close();
}
