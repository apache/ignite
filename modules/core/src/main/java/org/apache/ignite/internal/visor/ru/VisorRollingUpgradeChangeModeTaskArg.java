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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Represents arguments for {@link VisorRollingUpgradeChangeModeTask}.
 */
public class VisorRollingUpgradeChangeModeTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Rolling upgrade operation. */
    private VisorRollingUpgradeOperation op;

    /**
     * Strict validation mode.
     * This flag only makes sense when {@link VisorRollingUpgradeOperation#ENABLE} is used.
     */
    private boolean forcedMode;

    /**
     * Creates a new instance of VisorRollingUpgradeChangeModeTaskArg.
     */
    public VisorRollingUpgradeChangeModeTaskArg() {
    }

    /**
     * Creates a new instance of VisorRollingUpgradeChangeModeTaskArg with the given parameters.
     * @param op Rolling upgrade operation.
     * @param forcedMode {@code true} if forced mode enabled.
     *          This flag only makes sense when {@link VisorRollingUpgradeOperation#ENABLE} is used.
     */
    public VisorRollingUpgradeChangeModeTaskArg(VisorRollingUpgradeOperation op, boolean forcedMode) {
        this.op = op;
        this.forcedMode = forcedMode;
    }

    /**
     * @return Rolling upgrade operation.
     */
    public VisorRollingUpgradeOperation getOperation() {
        return op;
    }

    /**
     * @return {@code true} if forced mode is enabled.
     */
    public boolean isForcedMode() {
        return forcedMode;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, op);
        out.writeBoolean(forcedMode);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        op = VisorRollingUpgradeOperation.fromOrdinal(in.readByte());
        forcedMode = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorRollingUpgradeChangeModeTaskArg.class, this);
    }
}
