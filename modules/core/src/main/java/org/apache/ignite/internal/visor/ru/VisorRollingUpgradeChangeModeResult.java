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

package org.apache.ignite.internal.visor.ru;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.ru.RollingUpgradeModeChangeResult;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;

/**
 * Represents a wrapper class for a value of {@link RollingUpgradeModeChangeResult}.
 */
public class VisorRollingUpgradeChangeModeResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Rolling upgrade status. */
    private VisorRollingUpgradeStatus status;

    /** Overall status. */
    private RollingUpgradeModeChangeResult.Result res;

    /** The reason why the operation was failed. */
    private VisorExceptionWrapper cause;

    /**
     * Default constructor, required for serialization.
     */
    public VisorRollingUpgradeChangeModeResult() {
        // No-op.
    }

    /**
     * Creates a new wrapper that represents the given {@code changeModeRes}.
     *
     * @param changeModeRes Status of the operation.
     */
    public VisorRollingUpgradeChangeModeResult(RollingUpgradeModeChangeResult changeModeRes) {
        res = changeModeRes.result();
        status = new VisorRollingUpgradeStatus(changeModeRes.status());
        cause = (changeModeRes.cause() != null)? new VisorExceptionWrapper(changeModeRes.cause()) : null;
    }

    /**
     * Returns overall status of the operation.
     *
     * @return Status of the operation.
     */
    public RollingUpgradeModeChangeResult.Result getResult() {
        return res;
    }

    /**
     * Returns rolling upgrade status.
     *
     * @return Rolling upgrade status.
     */
    public VisorRollingUpgradeStatus getStatus() {
        return status;
    }

    /**
     * Returns the reason for the failed operation.
     *
     * @return Cause of the failure.
     */
    public VisorExceptionWrapper getCause() {
        return cause;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, res);
        out.writeObject(status);
        out.writeObject(cause);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        res = RollingUpgradeModeChangeResult.Result.fromOrdinal(in.readByte());
        status = (VisorRollingUpgradeStatus)in.readObject();
        cause = (VisorExceptionWrapper)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorRollingUpgradeChangeModeResult.class, this);
    }
}
