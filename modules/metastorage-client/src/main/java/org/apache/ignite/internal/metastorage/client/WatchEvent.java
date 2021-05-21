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

package org.apache.ignite.internal.metastorage.client;

import java.util.Collection;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * Watch event contains all entry updates done under one revision. Each particular entry update in this revision
 * is represented by {@link EntryEvent} entity.
 */
public class WatchEvent {
    /** Events about each entry update in the revision. */
    private final List<EntryEvent> entryEvts;

    /** Designates that watch event contains only one update revision. */
    private final boolean single;

    /**
     * Constructs an watch event with given entry events collection.
     *
     * @param entryEvts Events for entries corresponding to an update under one revision.
     */
    public WatchEvent(List<EntryEvent> entryEvts) {
        assert entryEvts != null && !entryEvts.isEmpty();

        this.single = entryEvts.size() == 1;
        this.entryEvts = entryEvts;
    }

    /**
     * Constructs watch event with single entry update.
     *
     * @param entryEvt Entry event.
     */
    public WatchEvent(@NotNull EntryEvent entryEvt) {
        this(List.of(entryEvt));
    }

    /**
     * Returns {@code true} if watch event contains only one entry event.
     *
     * @return {@code True} if watch event contains only one entry event.
     */
    public boolean single() {
        return single;
    }

    /**
     * Returns collection of entry entry event done under one revision.
     *
     * @return Collection of entry entry event done under one revision.
     */
    public Collection<EntryEvent> entryEvents() {
        return entryEvts;
    }

    /**
     * Returns entry event. It is useful method in case when we know that only one event was modified.
     *
     * @return Entry event.
     */
    public EntryEvent entryEvent() {
        return entryEvts.get(0);
    }
}
