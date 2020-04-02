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
import org.apache.ignite.internal.ThinProtocolFeature;

/**
 * Defines supported features for thin client.
 */
public enum ProtocolFeature implements ThinProtocolFeature {
    USER_ATTRIBUTES(0);

    /** */
    private static final EnumSet<ProtocolFeature> ALL_FEATURES_AS_ENUM_SET = EnumSet.allOf(ProtocolFeature.class);

    /** Feature id. */
    private final int featureId;

    /**
     * @param id Feature ID.
     */
    ProtocolFeature(int id) {
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
    public static EnumSet<ProtocolFeature> enumSet(byte[] bytes) {
        return ThinProtocolFeature.enumSet(bytes, ProtocolFeature.class);
    }

    /**
     * @return All features as a set.
     */
    public static EnumSet<ProtocolFeature> allFeaturesAsEnumSet() {
        return ALL_FEATURES_AS_ENUM_SET.clone();
    }

    /**
     * @return All features as byte array.
     */
    public static byte[] allFeaturesAsByteArray() {
        return ThinProtocolFeature.featuresAsBytes(allFeaturesAsEnumSet());
    }
}
