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

package org.apache.ignite.internal.client.thin;

import java.util.BitSet;
import java.util.Collection;
import java.util.EnumSet;

/**
 * Defines supported bitmask features for thin client.
 */
public enum ProtocolBitmaskFeature {
    /** Feature for user attributes. */
    USER_ATTRIBUTES(0),

    /** Compute tasks (execute by task name). */
    EXECUTE_TASK_BY_NAME(1),

    /**
     * Adds cluster states besides ACTIVE and INACTIVE.
     */
    CLUSTER_STATES(2),

    /** Cluster groups. */
    CLUSTER_GROUPS(4),

    /** Invoke service methods. */
    SERVICE_INVOKE(5);

    /** */
    private static final EnumSet<ProtocolBitmaskFeature> ALL_FEATURES_AS_ENUM_SET =
        EnumSet.allOf(ProtocolBitmaskFeature.class);

    /** Feature id. */
    private final int featureId;

    /**
     * @param id Feature ID.
     */
    ProtocolBitmaskFeature(int id) {
        featureId = id;
    }

    /**
     * @return Feature ID.
     */
    public int featureId() {
        return featureId;
    }

    /**
     * @param bytes Feature byte array.
     * @return Set of supported features.
     */
    public static EnumSet<ProtocolBitmaskFeature> enumSet(byte[] bytes) {
        EnumSet<ProtocolBitmaskFeature> set = EnumSet.noneOf(ProtocolBitmaskFeature.class);

        if (bytes == null)
            return set;

        final BitSet bSet = BitSet.valueOf(bytes);

        for (ProtocolBitmaskFeature e : ProtocolBitmaskFeature.values()) {
            if (bSet.get(e.featureId()))
                set.add(e);
        }

        return set;
    }

    /**
     * @param features Feature set.
     * @return Byte array representing all supported features.
     */
    static byte[] featuresAsBytes(Collection<ProtocolBitmaskFeature> features) {
        final BitSet set = new BitSet();

        for (ProtocolBitmaskFeature f : features)
            set.set(f.featureId());

        return set.toByteArray();
    }

    /**
     * @return All features as a set.
     */
    public static EnumSet<ProtocolBitmaskFeature> allFeaturesAsEnumSet() {
        return ALL_FEATURES_AS_ENUM_SET.clone();
    }
}
