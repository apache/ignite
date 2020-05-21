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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.EnumSet;
import org.apache.ignite.internal.ThinProtocolFeature;

/**
 * Defines supported features for JDBC thin client.
 */
public enum JdbcThinFeature implements ThinProtocolFeature {
    /** */
    RESERVED(0),

    /**
     * Whether to allow sending custom object through Thin JDBC protocol.
     */
    CUSTOM_OBJECT(1);

    /** */
    private static final EnumSet<JdbcThinFeature> ALL_FEATURES_AS_ENUM_SET = EnumSet.allOf(JdbcThinFeature.class);

    /** Feature id. */
    private final int featureId;

    /**
     * @param id Feature ID.
     */
    JdbcThinFeature(int id) {
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
    public static EnumSet<JdbcThinFeature> enumSet(byte[] bytes) {
        return ThinProtocolFeature.enumSet(bytes, JdbcThinFeature.class);
    }

    /** */
    public static EnumSet<JdbcThinFeature> allFeaturesAsEnumSet() {
        return ALL_FEATURES_AS_ENUM_SET.clone();
    }
}
