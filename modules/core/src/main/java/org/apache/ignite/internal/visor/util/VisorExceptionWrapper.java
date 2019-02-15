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

package org.apache.ignite.internal.visor.util;

import java.util.List;
import org.apache.ignite.internal.util.typedef.X;

/**
 * Exception wrapper for safe for transferring to Visor.
 */
public class VisorExceptionWrapper extends Throwable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Detail message string of this throwable */
    private String detailMsg;

    /** Simple class name of base throwable object. */
    private String clsSimpleName;

    /** Class name of base throwable object. */
    private String clsName;

    /**
     * Wrap throwable by presented on Visor throwable object.
     *
     * @param cause Base throwable object.
     */
    public VisorExceptionWrapper(Throwable cause) {
        assert cause != null;

        clsSimpleName = cause.getClass().getSimpleName();
        clsName = cause.getClass().getName();

        detailMsg = cause.getMessage();

        StackTraceElement[] stackTrace = cause.getStackTrace();

        if (stackTrace != null)
            setStackTrace(stackTrace);

        if (cause.getCause() != null)
            initCause(new VisorExceptionWrapper(cause.getCause()));

        List<Throwable> suppressed = X.getSuppressedList(cause);

        for (Throwable sup : suppressed)
            addSuppressed(new VisorExceptionWrapper(sup));
    }

    /**
     * @return Class simple name of base throwable object.
     */
    public String getClassSimpleName() {
        return clsSimpleName;
    }

    /**
     * @return Class name of base throwable object.
     */
    public String getClassName() {
        return clsName;
    }

    /** {@inheritDoc} */
    @Override public String getMessage() {
        return detailMsg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return (detailMsg != null) ? (clsName + ": " + detailMsg) : clsName;
    }
}
