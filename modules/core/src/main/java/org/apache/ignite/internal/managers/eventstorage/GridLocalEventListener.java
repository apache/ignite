/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.eventstorage;

import java.util.EventListener;
import org.apache.ignite.events.Event;

/**
 * Listener for asynchronous local node grid events. You can subscribe for local node grid
 * event notifications via {@link GridEventStorageManager#addLocalEventListener(GridLocalEventListener, int...)}.
 * <p>
 * Use {@link GridEventStorageManager#addLocalEventListener(org.apache.ignite.lang.IgnitePredicate, int...)} to register
 * this listener with grid.
 * @see org.apache.ignite.IgniteEvents#localListen(org.apache.ignite.lang.IgnitePredicate, int...)
 */
public interface GridLocalEventListener extends EventListener {
    /**
     * Local event callback.
     *
     * @param evt local grid event.
     */
    public void onEvent(Event evt);
}