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

import org.apache.ignite.internal.thread.context.function.ContextAwareCallable;
import org.apache.ignite.internal.thread.context.function.ContextAwareRunnable;

/**
 * Represents snapshot of all Attributes and their corresponding values for a particular {@link Context} instance.
 * Its main purpose to save Context state and restore it later, possible for Context bound to another thread.
 *
 * @see Context
 * @see Context#createSnapshot()
 * @see Context#restoreSnapshot(ContextSnapshot)
 * @see ContextAwareCallable
 * @see ContextAwareRunnable
 */
public interface ContextSnapshot {
    // No-op.
}
