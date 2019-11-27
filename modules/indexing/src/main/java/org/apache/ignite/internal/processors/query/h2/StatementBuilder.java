/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2;

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
    /** */
    private final StringBuilder builder = new StringBuilder();

    /** */
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

    /**
     * Return underlying builder.
     * @return underlying builder
     */
    public StringBuilder builder() {
        return builder;
    }
}
