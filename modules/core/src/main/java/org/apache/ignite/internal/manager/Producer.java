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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Interface which can produce its events.
 */
public abstract class Producer<T extends Event, P extends EventParameters> {
    /** All listeners. */
    private ConcurrentHashMap<T, ConcurrentLinkedQueue<EventListener<P>>> listeners = new ConcurrentHashMap<>();

    /**
     * Registers an event listener.
     * When the event predicate returns true it would never invoke after,
     * otherwise this predicate would receive an event again.
     *
     * @param evt Event.
     * @param closure Closure.
     */
    public void listen(T evt, EventListener<P> closure) {
        listeners.computeIfAbsent(evt, evtKey -> new ConcurrentLinkedQueue<>())
            .offer(closure);
    }

    /**
     * Removes a listener associated with the event.
     *
     * @param evt Event.
     * @param closure Closure.
     */
    public void removeListener(T evt, EventListener<P> closure) {
        removeListener(evt, closure, null);
    }

    /**
     * Removes a listener associated with the event.
     *
     * @param evt Event.
     * @param closure Closure.
     * @param cause The exception that was a cause which a listener is removed.
     */
    public void removeListener(T evt, EventListener<P> closure, @Nullable IgniteInternalCheckedException cause) {
        if (listeners.computeIfAbsent(evt, evtKey -> new ConcurrentLinkedQueue<>()).remove(closure))
            closure.remove(cause == null ? new ListenerRemovedException() : new ListenerRemovedException(cause));
    }

    /**
     * Notifies every listener that subscribed before.
     *
     * @param evt Event type.
     * @param params Event parameters.
     * @param err Exception when it was happened, or {@code null} otherwise.
     */
    protected void onEvent(T evt, P params, Throwable err) {
        ConcurrentLinkedQueue<EventListener<P>> queue = listeners.get(evt);

        if (queue == null)
            return;

        EventListener<P> closure;

        Iterator<EventListener<P>> iter = queue.iterator();

        while (iter.hasNext()) {
            closure = iter.next();

            if (closure.notify(params, err))
                iter.remove();
        }
    }
}
