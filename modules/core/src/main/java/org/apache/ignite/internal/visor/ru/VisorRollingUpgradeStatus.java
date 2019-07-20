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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.ru.RollingUpgradeStatus;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Represents a wrapper class for a value of {@link RollingUpgradeStatus}.
 */
public class VisorRollingUpgradeStatus extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@code true} if Rolling Upgrade is enabled. */
    private boolean enabled;

    /** {@code true} if forced mode is enabled. */
    private boolean forcedModeEnabled;

    /** Represents the version that is used as starting point for Rolling Upgrade. */
    private String initVer;

    /** Represents the resulting version. */
    private String targetVer;

    /** Feature set that is supported by nodes. */
    private Set<String> supportedFeatures;

    /**
     * Default constructor, required for serialization.
     */
    public VisorRollingUpgradeStatus() {
        // No-op.
    }

    /**
     * Creates a wrapper class that represents a value of {@link RollingUpgradeStatus}.
     *
     * @param enabled {@code true} if Rolling Upgrade is enabled.
     * @param forcedModeEnabled {@code true} if forced mode is enabled.
     * @param initVer Initial version.
     * @param targetVer Resulting version.
     * @param supportedFeatures Feature set that is supported by nodes.
     */
    public VisorRollingUpgradeStatus(
        boolean enabled,
        boolean forcedModeEnabled,
        String initVer,
        String targetVer,
        Set<String> supportedFeatures
    ) {
        this.enabled = enabled;
        this.forcedModeEnabled = forcedModeEnabled;
        this.initVer = initVer;
        this.targetVer = targetVer;
        this.supportedFeatures = supportedFeatures;
    }

    /**
     * Creates a wrapper class that represents a value of {@link RollingUpgradeStatus}.
     *
     * @param status Rolling upgrade status to be copied.
     */
    public VisorRollingUpgradeStatus(RollingUpgradeStatus status) {
        this(
            status.enabled(),
            status.forcedModeEnabled(),
            status.initialVersion().toString(),
            (status.targetVersion() != null)? status.targetVersion().toString(): "",
            status.supportedFeatures().stream().map(Enum::name).collect(Collectors.toSet()));
    }

    /**
     * Returns {@code true} if Rolling Upgrade is enabled and is in progress.
     *
     * @return {@code true} if Rolling Upgrade is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @return {@code true} if strict mode is disabled.
     */
    public boolean isForcedModeEnabled() {
        return forcedModeEnabled;
    }

    /**
     * Returns the version that is used as starting point for Rolling Upgrade.
     *
     * @return Initial version.
     */
    public String getInitialVersion() {
        return initVer;
    }

    /**
     * Returns the target version.
     * The returned value can be {@code null} if Rolling Upgrade is not in progress
     * or target version is not determined yet.
     *
     * This method makes sense only for the case when the {@code forced} mode is disabled.
     *
     * @return Target version.
     */
    public String getTargetVersion() {
        return targetVer;
    }

    /**
     * Returns a set of features that is supported by all nodes in the cluster.
     *
     * @return Feature set supported by all cluster nodes.
     */
    public Set<String> getSupportedFeatures() {
        return supportedFeatures;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeBoolean(forcedModeEnabled);
        U.writeString(out, initVer);
        U.writeString(out, targetVer);
        U.writeCollection(out, supportedFeatures);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        enabled = in.readBoolean();
        forcedModeEnabled = in.readBoolean();
        initVer = U.readString(in);
        targetVer = U.readString(in);
        supportedFeatures = U.readSet(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorRollingUpgradeStatus.class, this);
    }
}
