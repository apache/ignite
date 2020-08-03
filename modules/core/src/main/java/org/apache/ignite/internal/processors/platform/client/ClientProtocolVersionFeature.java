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

import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;

import static org.apache.ignite.internal.processors.platform.client.ClientConnectionContext.VER_1_1_0;
import static org.apache.ignite.internal.processors.platform.client.ClientConnectionContext.VER_1_2_0;
import static org.apache.ignite.internal.processors.platform.client.ClientConnectionContext.VER_1_4_0;
import static org.apache.ignite.internal.processors.platform.client.ClientConnectionContext.VER_1_6_0;
import static org.apache.ignite.internal.processors.platform.client.ClientConnectionContext.VER_1_7_0;

/**
 * Thin client feature that was introduced by introducing new protocol version.
 * Legacy approach. No new features of this kind should be added without strong justification. Use
 * {@link ClientBitmaskFeature} for all newly introduced features.
 */
public class ClientProtocolVersionFeature {
    /** Authorization feature. */
    public static final ClientProtocolVersionFeature AUTHORIZATION = new ClientProtocolVersionFeature(VER_1_1_0);

    /** Query entity precision and scale feature. */
    public static final ClientProtocolVersionFeature QUERY_ENTITY_PRECISION_AND_SCALE =
        new ClientProtocolVersionFeature(VER_1_2_0);

    /** Partition awareness feature. */
    public static final ClientProtocolVersionFeature PARTITION_AWARENESS = new ClientProtocolVersionFeature(VER_1_4_0);

    /** Expiry policy feature. */
    public static final ClientProtocolVersionFeature EXPIRY_POLICY = new ClientProtocolVersionFeature(VER_1_6_0);

    /** Bitmap features introduced. */
    public static final ClientProtocolVersionFeature BITMAP_FEATURES = new ClientProtocolVersionFeature(VER_1_7_0);

    /** Version in which the feature was introduced. */
    private final ClientListenerProtocolVersion ver;

    /**
     * @param ver Version in which the feature was introduced.
     */
    ClientProtocolVersionFeature(ClientListenerProtocolVersion ver) {
        this.ver = ver;
    }

    /**
     * @return Version in which the feature was introduced.
     */
    public ClientListenerProtocolVersion verIntroduced() {
        return ver;
    }
}
