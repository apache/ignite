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

/**
 * Defines supported features for thin client.
 */
public enum ClientBitmaskFeature implements ThinProtocolFeature {
    /** Feature for user attributes. */
    USER_ATTRIBUTES(0),

    /** Compute tasks (execute by task name). */
    EXECUTE_TASK_BY_NAME(1),

    /** Adds cluster states besides ACTIVE and INACTIVE. */
    CLUSTER_STATES(2),

    /** Client discovery. */
    CLUSTER_GROUP_GET_NODES_ENDPOINTS(3),

    /** Cluster groups. */
    CLUSTER_GROUPS(4),

    /** Service invocation. */
    SERVICE_INVOKE(5);

    /** */
    private static final EnumSet<ClientBitmaskFeature> ALL_FEATURES_AS_ENUM_SET =
        EnumSet.allOf(ClientBitmaskFeature.class);

    /** Feature id. */
    private final int featureId;

    /**
     * @param id Feature ID.
     */
    ClientBitmaskFeature(int id) {
        featureId = id;
    }

    /** {@inheritDoc} */
    @Override public int featureId() {
        return featureId;
    }

    /**
     * @param bytes Feature byte array.
     * @return Set of supported features.
     */
    public static EnumSet<ClientBitmaskFeature> enumSet(byte[] bytes) {
        return ThinProtocolFeature.enumSet(bytes, ClientBitmaskFeature.class);
    }

    /** */
    public static EnumSet<ClientBitmaskFeature> allFeaturesAsEnumSet() {
        return ALL_FEATURES_AS_ENUM_SET.clone();
    }
}
