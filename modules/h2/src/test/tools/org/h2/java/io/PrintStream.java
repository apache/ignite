/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java.io;

/**
 * A print stream.
 */
public class PrintStream {

    /**
     * Print the given string.
     *
     * @param s the string
     */
    @SuppressWarnings("unused")
    public void println(String s) {
        // c: int x = s->chars->length();
        // c: printf("%.*S\n", x, s->chars->getPointer());
    }

}
