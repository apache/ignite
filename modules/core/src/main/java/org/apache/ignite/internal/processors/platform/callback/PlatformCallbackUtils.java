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

package org.apache.ignite.internal.processors.platform.callback;

/**
 * Platform callback utility methods. Implemented in target platform. All methods in this class must be
 * package-visible and invoked only through {@link PlatformCallbackGateway}.
 */
public class PlatformCallbackUtils {
    /**
     * Redirects the console output.
     *
     * @param str String to write.
     * @param isErr Whether this is stdErr or stdOut.
     */
    static native void consoleWrite(String str, boolean isErr);

    /**
     * Logs to the native logger.
     *
     * @param envPtr Environment pointer.
     * @param level Log level.
     * @param message Message.
     * @param category Category.
     * @param errorInfo Error info.
     * @param memPtr Pointer to optional payload (serialized exception).
     */
    static native void loggerLog(long envPtr, int level, String message, String category, String errorInfo, long memPtr);

    /**
     * Gets a value indicating whether native logger has specified level enabled.
     *
     * @param envPtr Environment pointer.
     * @param level Log level.
     */
    static native boolean loggerIsLevelEnabled(long envPtr, int level);

    /**
     * Performs a generic long-long operation.
     *
     * @param envPtr Environment pointer.
     * @param type Operation code.
     * @param val Value.
     * @return Value.
     */
    static native long inLongOutLong(long envPtr, int type, long val);

    /**
     * Performs a generic out-in operation.
     *
     * @param envPtr Environment pointer.
     * @param type Operation code.
     * @param val1 First value.
     * @param val2 Second value.
     * @param val3 Third value.
     * @param arg Object argument.
     * @return Value.
     */
    static native long inLongLongLongObjectOutLong(long envPtr, int type, long val1, long val2, long val3, Object arg);

    /**
     * Private constructor.
     */
    private PlatformCallbackUtils() {
        // No-op.
    }
}
