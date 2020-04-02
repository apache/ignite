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

package org.apache.ignite.internal.client.thin;

import java.util.EnumSet;

import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_1_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_2_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_3_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_4_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_5_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_6_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_7_0;

/**
 * Protocol Context.
 */
public class ProtocolContext {
    /** Protocol version. */
    private final ProtocolVersion ver;

    /** Features. */
    private final EnumSet<ProtocolFeature> features;

    /**
     * @param ver Protocol version.
     * @param features Supported features.
     */
    public ProtocolContext(ProtocolVersion ver, EnumSet<ProtocolFeature> features) {
        this.ver = ver;
        this.features = features;
    }

    /**
     * @return {@code true} if protocol features supported.
     */
    public boolean isUserAttributesSupported() {
        return features.contains(ProtocolFeature.USER_ATTRIBUTES);
    }

    /**
     * @return {@code true} if protocol feature mask is supported.
     */
    public boolean isFeaturesSupported() {
        return isFeaturesSupported(ver);
    }

    /**
     * @return {@code true} if expiration policy supported.
     */
    public boolean isExpirationPolicySupported() {
        return ver.compareTo(V1_6_0) >= 0;
    }

    /**
     * @return {@code true} if transactions supported.
     */
    public boolean isTransactionsSupported() {
        return ver.compareTo(V1_5_0) >= 0;
    }

    /**
     * @return {@code true} if partition awareness supported.
     */
    public boolean isPartitionAwarenessSupported() {
        return ver.compareTo(V1_4_0) >= 0;
    }

    /**
     * @return {@code true} if idle timeout supported.
     */
    public boolean isIdleTimeoutSupported() {
        return ver.compareTo(V1_3_0) >= 0;
    }

    /**
     * @return {@code true} if handshake timeout supported.
     */
    public boolean isHandshakeTimeoutSupported() {
        return ver.compareTo(V1_3_0) >= 0;
    }

    /**
     * @return {@code true} if lazy memory allocation supported.
     */
    public boolean isLazyMemoryAllocationSupported() {
        return ver.compareTo(V1_3_0) >= 0;
    }

    /**
     * @return {@code true} if query entity precision and scale supported.
     */
    public boolean isQueryEntityPrecisionAndScaleSupported() {
        return ver.compareTo(V1_2_0) >= 0;
    }

    /**
     * @return {@code true} if encryption configuration supported.
     */
    public boolean isEncryptionConfigurationSupported() {
        return ver.compareTo(V1_2_0) >= 0;
    }

    /**
     * @return {@code true} if authorization supported.
     */
    public boolean isAuthorizationSupported() {
        return ver.compareTo(V1_1_0) >= 0;
    }

    /**
     * @return Supported features.
     */
    public EnumSet<ProtocolFeature> features() {
        return features;
    }

    /**
     * @return Protocol version.
     */
    public ProtocolVersion version() {
        return ver;
    }

    /**
     * @return {@code true} if protocol features supported.
     */
    public static boolean isFeaturesSupported(ProtocolVersion ver) {
        return ver.compareTo(V1_7_0) >= 0;
    }
}
