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

package org.apache.ignite.internal.processors.platform.client;

import java.util.EnumSet;
import org.apache.ignite.internal.ThinProtocolFeature;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;

import static org.apache.ignite.internal.processors.platform.client.ClientConnectionContext.*;

/**
 * Protocol context for JDBC protocol. Holds protocol version and supported features.
 */
public class ClientProtocolContext {
    /** Protocol version. */
    private final ClientListenerProtocolVersion ver;

    /** Features. */
    private final EnumSet<ClientFeature> features;

    /**
     * @param ver Protocol version.
     * @param features Supported features.
     */
    public ClientProtocolContext(ClientListenerProtocolVersion ver, EnumSet<ClientFeature> features) {
        this.ver = ver;
        this.features = features != null ? features : EnumSet.noneOf(ClientFeature.class);
    }

    /**
     * @return {@code true} if protocol features supported.
     */
    public boolean isUserAttributesSupported() {
        return features.contains(ClientFeature.USER_ATTRIBUTES);
    }

    /**
     * @return {@code true} if protocol features supported.
     */
    public boolean isFeaturesSupported() {
        return isFeaturesSupported(ver);
    }

    /**
     * @return {@code true} if expiration policy supported.
     */
    public boolean isExpirationPolicySupported() {
        return ver.compareTo(VER_1_6_0) >= 0;
    }

    /**
     * @return {@code true} if transactions supported.
     */
    public boolean isTransactionsSupported() {
        return ver.compareTo(VER_1_5_0) >= 0;
    }

    /**
     * @return {@code true} if partition awareness supported.
     */
    public boolean isPartitionAwarenessSupported() {
        return ver.compareTo(VER_1_4_0) >= 0;
    }

    /**
     * @return {@code true} if idle timeout supported.
     */
    public boolean isIdleTimeoutSupported() {
        return ver.compareTo(VER_1_3_0) >= 0;
    }

    /**
     * @return {@code true} if handshake timeout supported.
     */
    public boolean isHandshakeTimeoutSupported() {
        return ver.compareTo(VER_1_3_0) >= 0;
    }

    /**
     * @return {@code true} if lazy memory allocation supported.
     */
    public boolean isLazyMemoryAllocationSupported() {
        return ver.compareTo(VER_1_3_0) >= 0;
    }

    /**
     * @return {@code true} if query entity precision and scale supported.
     */
    public boolean isQueryEntityPrecisionAndScaleSupported() {
        return ver.compareTo(VER_1_2_0) >= 0;
    }

    /**
     * @return {@code true} if encryption configuration supported.
     */
    public boolean isEncryptionConfigurationSupported() {
        return ver.compareTo(VER_1_2_0) >= 0;
    }

    /**
     * @return {@code true} if authorization supported.
     */
    public boolean isAuthorizationSupported() {
        return ver.compareTo(VER_1_1_0) >= 0;
    }

    /**
     * @return Supported features.
     */
    public EnumSet<ClientFeature> features() {
        return features;
    }

    /**
     * @return Supported features as byte array.
     */
    public byte[] featureBytes() {
        return ThinProtocolFeature.featuresAsBytes(features);
    }

    /**
     * @return Protocol version.
     */
    public ClientListenerProtocolVersion version() {
        return ver;
    }

    /**
     * @return {@code true} if protocol features supported.
     */
    public static boolean isFeaturesSupported(ClientListenerProtocolVersion ver) {
        return ver.compareTo(VER_1_7_0) >= 0;
    }
}
