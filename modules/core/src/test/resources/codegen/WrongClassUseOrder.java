package org.apache.ignite.internal;

import org.apache.ignite.plugin.extensions.communication.Message;

public class WrongClassUseOrder implements Message {
    @Order(0)
    private int id;
}