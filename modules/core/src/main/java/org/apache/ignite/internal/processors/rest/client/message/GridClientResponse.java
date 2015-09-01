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