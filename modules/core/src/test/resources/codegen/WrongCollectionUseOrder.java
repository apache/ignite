package org.apache.ignite.internal;

import java.util.Set;
import org.apache.ignite.plugin.extensions.communication.Message;

public class WrongCollectionUseOrder implements Message {
    @Order(0)
    private Set<Integer> setValue;

    public Set<Integer> setValue() {
        return setValue;
    }

    public void setValue(Set<Integer> setValue) {
        this.setValue = setValue;
    }

    public short directType() {
        return 0;
    }

    public void onAckReceived() {
        // No-op.
    }
}
