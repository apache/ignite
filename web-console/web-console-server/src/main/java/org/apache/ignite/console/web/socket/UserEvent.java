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

package org.apache.ignite.console.web.socket;

import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Event to browser.
 */
public class UserEvent {
    /** User key. */
    private UserKey key;

    /** Event. */
    private WebSocketEvent evt;

    /**
     * @param key User key.
     * @param evt Event.
     */
    public UserEvent(UserKey key, WebSocketEvent evt) {
        this.key = key;
        this.evt = evt;
    }

    /**
     * @return value of user key.
     */
    public UserKey getKey() {
        return key;
    }

    /**
     * @return value of event.
     */
    public WebSocketEvent getEvt() {
        return evt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserEvent.class, this);
    }
}
