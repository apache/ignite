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

import org.apache.ignite.internal.thread.context.function.OperationContextAwareCallable;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareRunnable;

/**
 * Represents snapshot of all Attributes and their corresponding values for a particular {@link OperationContext}
 * instance. Its main purpose to save {@link OperationContext} state and restore it later, possible for
 * {@link OperationContext} bound to another thread.
 *
 * @see OperationContext
 * @see OperationContext#createSnapshot()
 * @see OperationContext#restoreSnapshot(OperationContextSnapshot)
 * @see OperationContextAwareCallable
 * @see OperationContextAwareRunnable
 */
public interface OperationContextSnapshot {
    // No-op.
}
