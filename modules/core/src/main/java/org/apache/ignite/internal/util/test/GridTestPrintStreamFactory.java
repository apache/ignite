/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util.test;

import java.io.PrintStream;

/**
 * Factory that allow to acquire/release Print Stream for test logging.
 */
public final class GridTestPrintStreamFactory {
    /** */
    private static final PrintStream sysOut = System.out;

    /** */
    private static final PrintStream sysErr = System.err;

    /** */
    private static GridTestPrintStream testOut;

    /** */
    private static GridTestPrintStream testErr;

    /** */
    private static long outCnt = 0;

    /** */
    private static long errCnt = 0;

    /**
     * Enforces singleton.
     */
    private GridTestPrintStreamFactory() {
        // No-op.
    }

    /**
     * Gets original standard out.
     *
     * @return Original standard out.
     */
    public static synchronized PrintStream getStdOut() {
        return sysOut;
    }

    /**
     * Gets original standard error.
     *
     * @return Original standard error.
     */
    public static synchronized PrintStream getStdErr() {
        return sysErr;
    }

    /**
     * Acquires output stream for logging tests.
     *
     * @return Junit out print stream.
     */
    public static synchronized GridTestPrintStream acquireOut() {
        // Lazy initialization is required here to ensure that parent
        // thread group is picked off correctly by implementation.
        if (testOut == null)
            testOut = new GridTestPrintStream(sysOut);

        if (outCnt == 0)
            System.setOut(testOut);

        outCnt++;

        return testOut;
    }

    /**
     * Acquires output stream for logging errors in tests.
     *
     * @return Junit error print stream.
     */
    public static synchronized GridTestPrintStream acquireErr() {
        // Lazy initialization is required here to ensure that parent
        // thread group is picked off correctly by implementation.
        if (testErr == null)
            testErr = new GridTestPrintStream(sysErr);

        if (errCnt == 0)
            System.setErr(testErr);

        errCnt++;

        return testErr;
    }

    /**
     * Releases standard out. If there are no more acquired standard outs,
     * then it is reset to its original value.
     */
    public static synchronized void releaseOut() {
        outCnt--;

        if (outCnt == 0)
            System.setOut(sysOut);
    }

    /**
     * Releases standard error. If there are no more acquired standard errors,
     * then it is reset to its original value.
     */
    public static synchronized void releaseErr() {
        errCnt--;

        if (errCnt == 0)
            System.setErr(sysErr);
    }
}