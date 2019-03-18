/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.coverage;

import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

/**
 * Helper class for the java file parser.
 */
public class Tokenizer {

    /**
     * This token type means no more tokens are available.
     */
    static final int TYPE_EOF = -1;

    private static final int TYPE_WORD = -2;
    private static final int TYPE_NOTHING = -3;
    private static final byte WHITESPACE = 1;
    private static final byte ALPHA = 4;
    private static final byte QUOTE = 8;

    private StringBuilder buffer;

    private Reader reader;

    private char[] chars = new char[20];
    private int peekChar;
    private int line = 1;

    private byte[] charTypes = new byte[256];

    private int type = TYPE_NOTHING;
    private String value;

    private Tokenizer() {
        wordChars('a', 'z');
        wordChars('A', 'Z');
        wordChars('0', '9');
        wordChars('.', '.');
        wordChars('+', '+');
        wordChars('-', '-');
        wordChars('_', '_');
        wordChars(128 + 32, 255);
        whitespaceChars(0, ' ');
        charTypes['"'] = QUOTE;
        charTypes['\''] = QUOTE;
    }

    Tokenizer(Reader r) {
        this();
        reader = r;
    }

    String getString() {
        return value;
    }

    private void wordChars(int low, int hi) {
        while (low <= hi) {
            charTypes[low++] |= ALPHA;
        }
    }

    private void whitespaceChars(int low, int hi) {
        while (low <= hi) {
            charTypes[low++] = WHITESPACE;
        }
    }

    private int read() throws IOException {
        int i = reader.read();
        if (i != -1) {
            append(i);
        }
        return i;
    }

    /**
     * Initialize the tokenizer.
     */
    void initToken() {
        buffer = new StringBuilder();
    }

    String getToken() {
        buffer.setLength(buffer.length() - 1);
        return buffer.toString();
    }

    private void append(int i) {
        buffer.append((char) i);
    }

    /**
     * Read the next token and get the token type.
     *
     * @return the token type
     */
    int nextToken() throws IOException {
        byte[] ct = charTypes;
        int c;
        value = null;

        if (type == TYPE_NOTHING) {
            c = read();
            if (c >= 0) {
                type = c;
            }
        } else {
            c = peekChar;
            if (c < 0) {
                try {
                    c = read();
                    if (c >= 0) {
                        type = c;
                    }
                } catch (EOFException e) {
                    c = -1;
                }
            }
        }

        if (c < 0) {
            return type = TYPE_EOF;
        }
        int charType = c < 256 ? ct[c] : ALPHA;
        while ((charType & WHITESPACE) != 0) {
            if (c == '\r') {
                line++;
                c = read();
                if (c == '\n') {
                    c = read();
                }
            } else {
                if (c == '\n') {
                    line++;
                }
                c = read();
            }
            if (c < 0) {
                return type = TYPE_EOF;
            }
            charType = c < 256 ? ct[c] : ALPHA;
        }
        if ((charType & ALPHA) != 0) {
            initToken();
            append(c);
            int i = 0;
            do {
                if (i >= chars.length) {
                    chars = Arrays.copyOf(chars, chars.length * 2);
                }
                chars[i++] = (char) c;
                c = read();
                charType = c < 0 ? WHITESPACE : c < 256 ? ct[c] : ALPHA;
            } while ((charType & ALPHA) != 0);
            peekChar = c;
            value = String.copyValueOf(chars, 0, i);
            return type = TYPE_WORD;
        }
        if ((charType & QUOTE) != 0) {
            initToken();
            append(c);
            type = c;
            int i = 0;
            // \octal needs a lookahead
            peekChar = read();
            while (peekChar >= 0 && peekChar != type && peekChar != '\n'
                    && peekChar != '\r') {
                if (peekChar == '\\') {
                    c = read();
                    // to allow \377, but not \477
                    int first = c;
                    if (c >= '0' && c <= '7') {
                        c = c - '0';
                        int c2 = read();
                        if ('0' <= c2 && c2 <= '7') {
                            c = (c << 3) + (c2 - '0');
                            c2 = read();
                            if ('0' <= c2 && c2 <= '7' && first <= '3') {
                                c = (c << 3) + (c2 - '0');
                                peekChar = read();
                            } else {
                                peekChar = c2;
                            }
                        } else {
                            peekChar = c2;
                        }
                    } else {
                        switch (c) {
                        case 'b':
                            c = '\b';
                            break;
                        case 'f':
                            c = '\f';
                            break;
                        case 'n':
                            c = '\n';
                            break;
                        case 'r':
                            c = '\r';
                            break;
                        case 't':
                            c = '\t';
                            break;
                        default:
                        }
                        peekChar = read();
                    }
                } else {
                    c = peekChar;
                    peekChar = read();
                }

                if (i >= chars.length) {
                    chars = Arrays.copyOf(chars, chars.length * 2);
                }
                chars[i++] = (char) c;
            }
            if (peekChar == type) {
                // keep \n or \r intact in peekChar
                peekChar = read();
            }
            value = String.copyValueOf(chars, 0, i);
            return type;
        }
        if (c == '/') {
            c = read();
            if (c == '*') {
                int prevChar = 0;
                while ((c = read()) != '/' || prevChar != '*') {
                    if (c == '\r') {
                        line++;
                        c = read();
                        if (c == '\n') {
                            c = read();
                        }
                    } else {
                        if (c == '\n') {
                            line++;
                            c = read();
                        }
                    }
                    if (c < 0) {
                        return type = TYPE_EOF;
                    }
                    prevChar = c;
                }
                peekChar = read();
                return nextToken();
            } else if (c == '/') {
                while ((c = read()) != '\n' && c != '\r' && c >= 0) {
                    // nothing
                }
                peekChar = c;
                return nextToken();
            } else {
                peekChar = c;
                return type = '/';
            }
        }
        peekChar = read();
        return type = c;
    }

    int getLine() {
        return line;
    }
}

