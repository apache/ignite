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
package org.apache.ignite.internal.commandline;

import org.jetbrains.annotations.NotNull;

/**
 *
 */
public enum OutputFormat {
    /** Single line. */
    SINGLE_LINE("single-line"),

    /** Multi line. */
    MULTI_LINE("multi-line");

    /** */
    private final String text;

    /** */
    OutputFormat(String text) {
        this.text = text;
    }

    /**
     * @return Text.
     */
    public String text() {
        return text;
    }

    /**
     * Converts format name in console to enumerated value.
     *
     * @param text Format name in console.
     * @return Enumerated value.
     * @throws IllegalArgumentException If enumerated value not found.
     */
    public static OutputFormat fromConsoleName(@NotNull String text) {
        for (OutputFormat format : values()) {
            if (format.text.equals(text))
                return format;
        }

        throw new IllegalArgumentException("Unknown output format " + text);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return text;
    }
}
