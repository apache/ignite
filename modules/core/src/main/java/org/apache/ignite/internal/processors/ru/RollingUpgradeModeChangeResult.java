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
package org.apache.ignite.internal.processors.ru;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Represents the result of changing the rolling upgrade mode.
 */
public class RollingUpgradeModeChangeResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Overall status of the operation.
     */
    public enum Status {
        /**
         * The changing of rolling upgrade mode successfully done.
         */
        SUCCESS,

        /**
         * The changing of rolling upgrade mode failed.
         */
        FAIL;

        /** Enumerated values. */
        private static final Status[] VALS = values();

        /**
         * Efficiently gets enumerated value from its ordinal.
         *
         * @param ord Ordinal value.
         * @return Enumerated value or {@code null} if ordinal out of range.
         */
        @Nullable public static Status fromOrdinal(int ord) {
            return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
        }
    }

    /** Overall status. */
    private Status status;

    /** The reason why the operation was failed. */
    private Exception cause;

    /**
     * Creates a new instance with the given {@code status}.
     */
    public RollingUpgradeModeChangeResult() {
    }

    /**
     * Creates a new instance with the given {@code status}.
     *
     * @param status status of the operation.
     */
    public RollingUpgradeModeChangeResult(Status status) {
        this.status = status;
    }

    /**
     * Creates a new instance with the given {@code status} and {@code cause}.
     *
     * @param status status of the operation.
     * @param cause cause of failure.
     */
    public RollingUpgradeModeChangeResult(Status status, Exception cause) {
        this.status = status;
        this.cause = cause;
    }

    /**
     * Returns overall status of the operation.
     *
     * @return status of the operation.
     */
    public Status status() {
        return status;
    }

    /**
     * Returns the reason for the failed operation.
     *
     * @return cause of the failure.
     */
    public Exception cause() {
        return cause;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, status);
        out.writeObject(cause);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        status = Status.fromOrdinal(in.readByte());
        cause = (Exception)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RollingUpgradeModeChangeResult.class, this);
    }
}
