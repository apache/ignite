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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Bean representing client operation result.
 */
public class GridClientResponse extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Command succeeded. */
    public static final int STATUS_SUCCESS = 0;

    /** Command failed. */
    public static final int STATUS_FAILED = 1;

    /** Authentication failure. */
    public static final int STATUS_AUTH_FAILURE = 2;

    /** Operation security failure. */
    public static final int STATUS_SECURITY_CHECK_FAILED = 3;

    /** Success flag */
    private int successStatus;

    /** Error message, if any. */
    private String errorMsg;

    /** Result object. */
    private Object res;

    /**
     * @return {@code True} if this request was successful.
     */
    public int successStatus() {
        return successStatus;
    }

    /**
     * @param successStatus Whether request was successful.
     */
    public void successStatus(int successStatus) {
        this.successStatus = successStatus;
    }

    /**
     * @return Error message, if any error occurred, or {@code null}.
     */
    public String errorMessage() {
        return errorMsg;
    }

    /**
     * @param errorMsg Error message, if any error occurred.
     */
    public void errorMessage(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    /**
     * @return Request result.
     */
    public Object result() {
        return res;
    }

    /**
     * @param res Request result.
     */
    public void result(Object res) {
        this.res = res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(successStatus);

        U.writeString(out, errorMsg);

        out.writeObject(res);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        successStatus = in.readInt();

        errorMsg = U.readString(in);

        res = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [clientId=" + clientId() + ", reqId=" + requestId() + ", " +
            "destId=" + destinationId() + ", status=" + successStatus + ", errMsg=" + errorMessage() +
            ", result=" + res + "]";
    }
}