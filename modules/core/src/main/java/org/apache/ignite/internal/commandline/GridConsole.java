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
 * Interface with {@link Console} methods and contract.
 */
public interface GridConsole {
    /** See {@link Console#writer()}. */
    PrintWriter writer();

    /** See {@link Console#reader()}. */
    Reader reader();

    /** See {@link Console#format(String, Object...)}. */
    Console format(String fmt, Object... args);

    /** See {@link Console#printf(String, Object...)}. */
    Console printf(String format, Object... args);

    /** See {@link Console#readLine(String, Object...)}. */
    String readLine(String fmt, Object... args);

    /** See {@link Console#readLine()}. */
    String readLine();

    /** See {@link Console#readPassword(String, Object...)}. */
    char[] readPassword(String fmt, Object... args);

    /** See {@link Console#readPassword()}. */
    char[] readPassword();

    /** See {@link Console#flush()}. */
    void flush();
}
