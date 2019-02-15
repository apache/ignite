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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;

/**
 * Data transfer object for suppressed errors.
 */
public class VisorSuppressedError extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long order;

    /** */
    @GridToStringExclude
    private VisorExceptionWrapper error;

    /** */
    private long threadId;

    /** */
    private String threadName;

    /** */
    private long time;

    /** */
    private String msg;

    /**
     * Default constructor.
     */
    public VisorSuppressedError() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param order Locally unique ID that is atomically incremented for each new error.
     * @param error Suppressed error.
     * @param msg Message that describe reason why error was suppressed.
     * @param threadId Thread ID.
     * @param threadName Thread name.
     * @param time Occurrence time.
     */
    public VisorSuppressedError(long order, VisorExceptionWrapper error, String msg, long threadId, String threadName, long time) {
        this.order = order;
        this.error = error;
        this.threadId = threadId;
        this.threadName = threadName;
        this.time = time;
        this.msg = msg;
    }

    /**
     * @return Locally unique ID that is atomically incremented for each new error.
     */
    public long getOrder() {
        return order;
    }

    /**
     * @return Gets message that describe reason why error was suppressed.
     */
    public String getMessage() {
        return msg;
    }

    /**
     * @return Suppressed error.
     */
    public VisorExceptionWrapper getError() {
        return error;
    }

    /**
     * @return Gets thread ID.
     */
    public long getThreadId() {
        return threadId;
    }

    /**
     * @return Gets thread name.
     */
    public String getThreadName() {
        return threadName;
    }

    /**
     * @return Gets time.
     */
    public long getTime() {
        return time;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(order);
        out.writeObject(error);
        out.writeLong(threadId);
        U.writeString(out, threadName);
        out.writeLong(time);
        U.writeString(out, msg);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        order = in.readLong();
        error = (VisorExceptionWrapper)in.readObject();
        threadId = in.readLong();
        threadName= U.readString(in);
        time = in.readLong();
        msg = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorSuppressedError.class, this);
    }
}
