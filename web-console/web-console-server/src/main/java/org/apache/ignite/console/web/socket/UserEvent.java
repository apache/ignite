

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
