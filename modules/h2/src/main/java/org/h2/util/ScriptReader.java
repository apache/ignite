/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import org.h2.engine.Constants;
import org.h2.message.DbException;

/**
 * This class can split SQL scripts to single SQL statements.
 * Each SQL statement ends with the character ';', however it is ignored
 * in comments and quotes.
 */
public class ScriptReader implements Closeable {

    private final Reader reader;
    private char[] buffer;

    /**
     * The position in the buffer of the next char to be read
     */
    private int bufferPos;

    /**
     * The position in the buffer of the statement start
     */
    private int bufferStart = -1;

    /**
     * The position in the buffer of the last available char
     */
    private int bufferEnd;

    /**
     * True if we have read past the end of file
     */
    private boolean endOfFile;

    /**
     * True if we are inside a comment
     */
    private boolean insideRemark;

    /**
     * Only valid if insideRemark is true. True if we are inside a block
     * comment, false if we are inside a line comment
     */
    private boolean blockRemark;

    /**
     * True if comments should be skipped completely by this reader.
     */
    private boolean skipRemarks;

    /**
     * The position in buffer of start of comment
     */
    private int remarkStart;

    /**
     * Create a new SQL script reader from the given reader
     *
     * @param reader the reader
     */
    public ScriptReader(Reader reader) {
        this.reader = reader;
        buffer = new char[Constants.IO_BUFFER_SIZE * 2];
    }

    /**
     * Close the underlying reader.
     */
    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    /**
     * Read a statement from the reader. This method returns null if the end has
     * been reached.
     *
     * @return the SQL statement or null
     */
    public String readStatement() {
        if (endOfFile) {
            return null;
        }
        try {
            return readStatementLoop();
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private String readStatementLoop() throws IOException {
        bufferStart = bufferPos;
        int c = read();
        while (true) {
            if (c < 0) {
                endOfFile = true;
                if (bufferPos - 1 == bufferStart) {
                    return null;
                }
                break;
            } else if (c == ';') {
                break;
            }
            switch (c) {
            case '$': {
                c = read();
                if (c == '$' && (bufferPos - bufferStart < 3 || buffer[bufferPos - 3] <= ' ')) {
                    // dollar quoted string
                    while (true) {
                        c = read();
                        if (c < 0) {
                            break;
                        }
                        if (c == '$') {
                            c = read();
                            if (c < 0) {
                                break;
                            }
                            if (c == '$') {
                                break;
                            }
                        }
                    }
                    c = read();
                }
                break;
            }
            case '\'':
                while (true) {
                    c = read();
                    if (c < 0) {
                        break;
                    }
                    if (c == '\'') {
                        break;
                    }
                }
                c = read();
                break;
            case '"':
                while (true) {
                    c = read();
                    if (c < 0) {
                        break;
                    }
                    if (c == '\"') {
                        break;
                    }
                }
                c = read();
                break;
            case '/': {
                c = read();
                if (c == '*') {
                    // block comment
                    startRemark(true);
                    while (true) {
                        c = read();
                        if (c < 0) {
                            break;
                        }
                        if (c == '*') {
                            c = read();
                            if (c < 0) {
                                clearRemark();
                                break;
                            }
                            if (c == '/') {
                                endRemark();
                                break;
                            }
                        }
                    }
                    c = read();
                } else if (c == '/') {
                    // single line comment
                    startRemark(false);
                    while (true) {
                        c = read();
                        if (c < 0) {
                            clearRemark();
                            break;
                        }
                        if (c == '\r' || c == '\n') {
                            endRemark();
                            break;
                        }
                    }
                    c = read();
                }
                break;
            }
            case '-': {
                c = read();
                if (c == '-') {
                    // single line comment
                    startRemark(false);
                    while (true) {
                        c = read();
                        if (c < 0) {
                            clearRemark();
                            break;
                        }
                        if (c == '\r' || c == '\n') {
                            endRemark();
                            break;
                        }
                    }
                    c = read();
                }
                break;
            }
            default: {
                c = read();
            }
            }
        }
        return new String(buffer, bufferStart, bufferPos - 1 - bufferStart);
    }

    private void startRemark(boolean block) {
        blockRemark = block;
        remarkStart = bufferPos - 2;
        insideRemark = true;
    }

    private void endRemark() {
        clearRemark();
        insideRemark = false;
    }

    private void clearRemark() {
        if (skipRemarks) {
            Arrays.fill(buffer, remarkStart, bufferPos, ' ');
        }
    }

    private int read() throws IOException {
        if (bufferPos >= bufferEnd) {
            return readBuffer();
        }
        return buffer[bufferPos++];
    }

    private int readBuffer() throws IOException {
        if (endOfFile) {
            return -1;
        }
        int keep = bufferPos - bufferStart;
        if (keep > 0) {
            char[] src = buffer;
            if (keep + Constants.IO_BUFFER_SIZE > src.length) {
                // protect against NegativeArraySizeException
                if (src.length >= Integer.MAX_VALUE / 2) {
                    throw new IOException("Error in parsing script, " +
                            "statement size exceeds 1G, " +
                            "first 80 characters of statement looks like: " +
                            new String(buffer, bufferStart, 80));
                }
                buffer = new char[src.length * 2];
            }
            System.arraycopy(src, bufferStart, buffer, 0, keep);
        }
        remarkStart -= bufferStart;
        bufferStart = 0;
        bufferPos = keep;
        int len = reader.read(buffer, keep, Constants.IO_BUFFER_SIZE);
        if (len == -1) {
            // ensure bufferPos > bufferEnd
            bufferEnd = -1024;
            endOfFile = true;
            // ensure the right number of characters are read
            // in case the input buffer is still used
            bufferPos++;
            return -1;
        }
        bufferEnd = keep + len;
        return buffer[bufferPos++];
    }

    /**
     * Check if this is the last statement, and if the single line or block
     * comment is not finished yet.
     *
     * @return true if the current position is inside a remark
     */
    public boolean isInsideRemark() {
        return insideRemark;
    }

    /**
     * If currently inside a remark, this method tells if it is a block comment
     * (true) or single line comment (false)
     *
     * @return true if inside a block comment
     */
    public boolean isBlockRemark() {
        return blockRemark;
    }

    /**
     * If comments should be skipped completely by this reader.
     *
     * @param skipRemarks true if comments should be skipped
     */
    public void setSkipRemarks(boolean skipRemarks) {
        this.skipRemarks = skipRemarks;
    }

}
