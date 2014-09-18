/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.tools.ant.beautifier;

/**
 * Character-based lexical token reader.
 */
class GridJavadocCharArrayLexReader {
    /** End Of File (EOF) constant. */
    public static final char EOF = (char)-1;

    /** Character-based buffer. */
    private char[] chars;

    /** Index in {@link #chars}. */
    private int index;

    /**
     * Creates reader with given buffer.
     *
     * @param chars Input character buffer.
     */
    GridJavadocCharArrayLexReader(char[] chars) {
        this.chars = chars;
    }

    /**
     * Gets length of the buffer.
     *
     * @return Length if the buffer.
     */
    int getLength() {
        return chars.length;
    }

    /**
     * Reads next character.
     *
     * @return Next character from the buffer.
     */
    int read() {
        return index == chars.length ? EOF : chars[index++];
    }

    /**
     * Peeks at the next character in the buffer.
     *
     * @return Next character that will be returned by next {@link #read()} apply.
     */
    int peek() {
        return index == chars.length ? EOF : chars[index];
    }

    /**
     * Skips next character in the buffer.
     */
    void skip() {
        if (index < chars.length) {
            index++;
        }
    }

    /**
     * Puts back last read character.
     */
    void back() {
        if (index > 0) {
            index--;
        }
    }

    /**
     * Tests whether buffer has more characters.
     *
     * @return {@code true} if buffer has at least one more character - {@code false} otherwise.
     */
    boolean hasMore() {
        return index < chars.length;
    }
}
