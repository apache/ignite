/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import java.io.Console;
import java.io.PrintWriter;
import java.io.Reader;

/**
 * Default implementation of {@link GridConsole}.
 */
public class NoopConsole implements GridConsole {
    /** {@inheritDoc} */
    @Override public PrintWriter writer() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Reader reader() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Console format(String fmt, Object... args) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Console printf(String format, Object... args) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String readLine(String fmt, Object... args) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String readLine() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public char[] readPassword(String fmt, Object... args) {
        return new char[0];
    }

    /** {@inheritDoc} */
    @Override public char[] readPassword() {
        return new char[0];
    }

    /** {@inheritDoc} */
    @Override public void flush() {
        /* No-op. */
    }
}
