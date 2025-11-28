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
 * Represents Snapshot of all {@link ContextAttribute}s and their corresponding values. Note that Snapshot also must
 * store he states of {@link ContextAttribute}s for which value are note explicitly specified.
 */
public interface ContextSnapshot {
    /** */
    ContextSnapshot EMPTY = new ContextSnapshot() {
        @Override public Scope restore() {
            return Scope.NOOP_SCOPE;
        }

        @Override public boolean isEmpty() {
            return true;
        }
    };

    /**
     * Stashes all {@link ContextAttribute} values attached to the thread from which this method is called and replaces
     * them with ones stored in the current Snapshot.
     *
     * @return {@link Scope} instance that, when closed, restores the values of all {@link ContextAttribute}s to the
     * state they were in before the current method was called.
     */
    public Scope restore();

    /** @return Whether snapshot is empty. */
    public boolean isEmpty();
}
