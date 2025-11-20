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
 * Represents an arbitrary Scope. A Scope is active from the moment it is created until the {@link #close()} method is
 * called on the {@link Scope} instance. It is strongly encouraged to use a try-with-resources block to close a Scope.
 */
public interface Scope extends AutoCloseable {
    /** Scope instance that does nothing when closed. */
    Scope NOOP_SCOPE = () -> {};

    /** Closes the scope. This operation cannot fail. */
    @Override void close();
}
