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

package org.apache.ignite.compute;

import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * This exception is thrown when user's code throws undeclared runtime exception. By user code it is
 * assumed the code in grid task, grid job or SPI. In most cases it should be an indication of unrecoverable
 * error condition such as assertion, {@link NullPointerException}, {@link OutOfMemoryError}, etc.
 */
public class ComputeUserUndeclaredException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates user undeclared exception with given task execution ID and
     * error message.
     *
     * @param msg Error message.
     */
    public ComputeUserUndeclaredException(String msg) {
        super(msg);
    }

    /**
     * Creates new user undeclared exception given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public ComputeUserUndeclaredException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates user undeclared exception with given task execution ID,
     * error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public ComputeUserUndeclaredException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}