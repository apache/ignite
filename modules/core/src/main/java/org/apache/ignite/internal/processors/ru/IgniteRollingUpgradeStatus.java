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
import java.util.Set;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Represent cluster-wide status of Rolling Upgrade process.
 */
public class IgniteRollingUpgradeStatus extends IgniteDataTransferObject implements RollingUpgradeStatus {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@code true} if Rolling Upgrade is enabled. */
    private boolean enabled;

    /** {@code true} if forced mode is enabled. */
    private boolean forcedModeEnabled;

    /** Represents the version that is used as starting point for Rolling Upgrade. */
    private IgniteProductVersion initVer;

    /** Represents the resulting version.*/
    private IgniteProductVersion updateVer;

    /** Feature set that is supported by nodes. */
    private Set<IgniteFeatures> supportedFeatures;

    /**
     * Creates a new instance of IgniteRollingUpgradeStatus.
     */
    public IgniteRollingUpgradeStatus() {
    }

    /**
     * Creates a new instance of the Rolling Upgrade status with the given parameters.
     *
     * @param enabled {@code true} if Rolling Upgrade is enabled.
     * @param forcedModeEnabled {@code true} if forced mode is enabled.
     * @param initVer Initial version.
     * @param updateVer Resulting version.
     * @param supportedFeatures Feature set that is supported by nodes.
     */
    public IgniteRollingUpgradeStatus(
        boolean enabled,
        boolean forcedModeEnabled,
        IgniteProductVersion initVer,
        IgniteProductVersion updateVer,
        Set<IgniteFeatures> supportedFeatures
    ) {
        assert enabled || !forcedModeEnabled: "Forced mode cannot be enabled if Rolling Upgrade is disabled.";

        this.enabled = enabled;
        this.forcedModeEnabled = forcedModeEnabled;
        this.initVer = initVer;
        this.updateVer = updateVer;
        this.supportedFeatures = supportedFeatures;
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return enabled;
    }

    /** {@inheritDoc} */
    @Override public boolean forcedModeEnabled() {
        return forcedModeEnabled;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion initialVersion() {
        return initVer;
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteProductVersion targetVersion() {
        return updateVer;
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteFeatures> supportedFeatures() {
        return supportedFeatures;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeBoolean(forcedModeEnabled);
        out.writeObject(initVer);
        out.writeObject(updateVer);
        out.writeObject(supportedFeatures);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        enabled = in.readBoolean();
        forcedModeEnabled = in.readBoolean();
        initVer = (IgniteProductVersion)in.readObject();
        updateVer = (IgniteProductVersion)in.readObject();
        supportedFeatures = (Set)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteRollingUpgradeStatus.class, this);
    }
}
