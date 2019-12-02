/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

import org.h2.engine.Constants;

/**
 * The reader input stream wraps a reader and convert the character to the UTF-8
 * format.
 */
public class ReaderInputStream extends InputStream {

    private final Reader reader;
    private final char[] chars;
    private final ByteArrayOutputStream out;
    private final Writer writer;
    private int pos;
    private int remaining;
    private byte[] buffer;

    public ReaderInputStream(Reader reader) {
        chars = new char[Constants.IO_BUFFER_SIZE];
        this.reader = reader;
        out = new ByteArrayOutputStream(Constants.IO_BUFFER_SIZE);
        writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
    }

    private void fillBuffer() throws IOException {
        if (remaining == 0) {
            pos = 0;
            remaining = reader.read(chars, 0, Constants.IO_BUFFER_SIZE);
            if (remaining < 0) {
                return;
            }
            writer.write(chars, 0, remaining);
            writer.flush();
            buffer = out.toByteArray();
            remaining = buffer.length;
            out.reset();
        }
    }

    @Override
    public int read() throws IOException {
        if (remaining == 0) {
            fillBuffer();
        }
        if (remaining < 0) {
            return -1;
        }
        remaining--;
        return buffer[pos++] & 0xff;
    }

}
