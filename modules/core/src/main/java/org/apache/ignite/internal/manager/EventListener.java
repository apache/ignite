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

package org.apache.ignite.internal.manager;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The listener handles events from a producer.
 *
 * @see Producer#listen(Event, EventListener)
 */
public interface EventListener<P extends EventParameters> {
    /**
     * Notifies the listener about an event.
     *
     * @param parameters Parameters provide a properties of the event. This attribute cannot be {@code null}.
     * @param exception  Exception which is happened during the event produced or {@code null}.
     * @return {@code True} means that the event is handled and a listener will be removed, {@code false} is the listener will stay listen.
     */
    boolean notify(@NotNull P parameters, @Nullable Throwable exception);

    /**
     * Removes an listener from the event producer. When the listener was removed it never receive a notification any more.
     *
     * @param exception An exception which was the reason that the listener was removed. It cannot be {@code null}.
     */
    void remove(@NotNull Throwable exception);
}
