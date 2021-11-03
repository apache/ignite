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

package org.apache.ignite.internal.client;

import java.util.EnumSet;
import org.apache.ignite.client.IgniteClientFeatureNotSupportedByServerException;
import org.apache.ignite.internal.client.proto.ProtocolVersion;

/**
 * Protocol Context.
 */
public class ProtocolContext {
    /** Protocol version. */
    private final ProtocolVersion ver;

    /** Features. */
    private final EnumSet<ProtocolBitmaskFeature> features;

    /**
     * @param ver      Protocol version.
     * @param features Supported features.
     */
    public ProtocolContext(ProtocolVersion ver, EnumSet<ProtocolBitmaskFeature> features) {
        this.ver = ver;
        this.features = features != null ? features : EnumSet.noneOf(ProtocolBitmaskFeature.class);
    }

    /**
     * Gets a value indicating whether a feature is supported.
     *
     * @param feature Feature.
     * @return {@code true} if bitmask protocol feature supported.
     */
    public boolean isFeatureSupported(ProtocolBitmaskFeature feature) {
        return features.contains(feature);
    }

    /**
     * Check that feature is supported by the server.
     *
     * @param feature Feature.
     * @throws IgniteClientFeatureNotSupportedByServerException If feature is not supported by the server.
     */
    public void checkFeatureSupported(ProtocolBitmaskFeature feature) throws IgniteClientFeatureNotSupportedByServerException {
        if (!isFeatureSupported(feature)) {
            throw new IgniteClientFeatureNotSupportedByServerException(feature);
        }
    }

    /**
     * @return Supported features.
     */
    public EnumSet<ProtocolBitmaskFeature> features() {
        return features;
    }

    /**
     * @return Protocol version.
     */
    public ProtocolVersion version() {
        return ver;
    }
}
