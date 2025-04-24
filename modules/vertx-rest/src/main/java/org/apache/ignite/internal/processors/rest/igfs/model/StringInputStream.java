package org.apache.ignite.internal.processors.rest.igfs.model;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;


/**
 * Simple wrapper for ByteArrayInputStream that will automatically encode the
 * string as UTF-8 data, and still allows access to the original string.
 */
public class StringInputStream extends ByteArrayInputStream {

    private final String string;

    public StringInputStream(String s) {
        super(s.getBytes(StandardCharsets.UTF_8));
        this.string = s;
    }

    /**
     * Returns the original string specified when this input stream was
     * constructed.
     *
     * @return The original string specified when this input stream was
     *         constructed.
     */
    public String getString() {
        return string;
    }
}

