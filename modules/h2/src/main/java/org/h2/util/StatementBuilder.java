/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

/**
 * A utility class to build a statement. In addition to the methods supported by
 * StringBuilder, it allows to add a text only in the second iteration. This
 * simplified constructs such as:
 * <pre>
 * StringBuilder buff = new StringBuilder();
 * for (int i = 0; i &lt; args.length; i++) {
 *     if (i &gt; 0) {
 *         buff.append(&quot;, &quot;);
 *     }
 *     buff.append(args[i]);
 * }
 * </pre>
 * to
 * <pre>
 * StatementBuilder buff = new StatementBuilder();
 * for (String s : args) {
 *     buff.appendExceptFirst(&quot;, &quot;);
 *     buff.append(a);
 * }
 *</pre>
 */
public class StatementBuilder {

    private final StringBuilder builder = new StringBuilder();
    private int index;

    /**
     * Create a new builder.
     */
    public StatementBuilder() {
        // nothing to do
    }

    /**
     * Create a new builder.
     *
     * @param string the initial string
     */
    public StatementBuilder(String string) {
        builder.append(string);
    }

    /**
     * Append a text.
     *
     * @param s the text to append
     * @return itself
     */
    public StatementBuilder append(String s) {
        builder.append(s);
        return this;
    }

    /**
     * Append a character.
     *
     * @param c the character to append
     * @return itself
     */
    public StatementBuilder append(char c) {
        builder.append(c);
        return this;
    }

    /**
     * Append a number.
     *
     * @param x the number to append
     * @return itself
     */
    public StatementBuilder append(long x) {
        builder.append(x);
        return this;
    }

    /**
     * Reset the loop counter.
     *
     * @return itself
     */
    public StatementBuilder resetCount() {
        index = 0;
        return this;
    }

    /**
     * Append a text, but only if appendExceptFirst was never called.
     *
     * @param s the text to append
     */
    public void appendOnlyFirst(String s) {
        if (index == 0) {
            builder.append(s);
        }
    }

    /**
     * Append a text, except when this method is called the first time.
     *
     * @param s the text to append
     */
    public void appendExceptFirst(String s) {
        if (index++ > 0) {
            builder.append(s);
        }
    }

    @Override
    public String toString() {
        return builder.toString();
    }

    /**
     * Get the length.
     *
     * @return the length
     */
    public int length() {
        return builder.length();
    }

}
