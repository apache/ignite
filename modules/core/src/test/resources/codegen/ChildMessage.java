package org.apache.ignite.internal;

import java.lang.String;
import java.nio.ByteBuffer;

public class ChildMessage extends AbstractMessage {
    @Order(1)
    private String str;

    public String str() {
        return str;
    }

    public void str(String str) {
        this.str = str;
    }
}
