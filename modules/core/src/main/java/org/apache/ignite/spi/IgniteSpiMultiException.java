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

package org.apache.ignite.spi;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * Grid SPI exception which may contain more than one failure.
 */
public class IgniteSpiMultiException extends IgniteSpiException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Nested exceptions. */
    private List<Throwable> causes = new ArrayList<>();

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public IgniteSpiMultiException(String msg) {
        super(msg);
    }

    /**
     * Creates new grid exception with given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public IgniteSpiMultiException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteSpiMultiException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     * @param nestedCauses Optional collection of nested causes.
     */
    public IgniteSpiMultiException(String msg, @Nullable Throwable cause, @Nullable Collection<Throwable> nestedCauses) {
        super(msg, cause);

        if (nestedCauses != null)
            causes.addAll(nestedCauses);
    }

    /**
     * Adds a new cause for multi-exception.
     *
     * @param cause Cause to add.
     */
    public void add(Throwable cause) {
        causes.add(cause);
    }

    /**
     * Gets nested causes for this multi-exception.
     *
     * @return Nested causes for this multi-exception.
     */
    public List<Throwable> nestedCauses() {
        return causes;
    }

    /** {@inheritDoc} */
    @Override public void printStackTrace(PrintStream s) {
        super.printStackTrace(s);

        for (Throwable t : causes)
            t.printStackTrace(s);
    }
}