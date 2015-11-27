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

package org.apache.ignite.binary;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;

/**
 * Defines configuration properties for a specific binary type. Providing per-type
 * configuration is optional, as it is generally enough, and also optional, to provide global binary
 * configuration using {@link IgniteConfiguration#setBinaryConfiguration(BinaryConfiguration)}.
 * However, this class allows you to change configuration properties for a specific
 * binary type without affecting configuration for other binary types.
 */
public class BinaryTypeConfiguration {
    /** Class name. */
    private String typeName;

    /** ID mapper. */
    private BinaryIdMapper idMapper;

    /** Serializer. */
    private BinarySerializer serializer;

    /** Enum flag. */
    private boolean isEnum;

    /**
     */
    public BinaryTypeConfiguration() {
        // No-op.
    }

    /**
     * @param typeName Class name.
     */
    public BinaryTypeConfiguration(String typeName) {
        this.typeName = typeName;
    }

    /**
     * Gets type name.
     *
     * @return Type name.
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * Sets type name.
     *
     * @param typeName Type name.
     */
    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    /**
     * Gets ID mapper.
     *
     * @return ID mapper.
     */
    public BinaryIdMapper getIdMapper() {
        return idMapper;
    }

    /**
     * Sets ID mapper.
     *
     * @param idMapper ID mapper.
     */
    public void setIdMapper(BinaryIdMapper idMapper) {
        this.idMapper = idMapper;
    }

    /**
     * Gets serializer.
     *
     * @return Serializer.
     */
    public BinarySerializer getSerializer() {
        return serializer;
    }

    /**
     * Sets serializer.
     *
     * @param serializer Serializer.
     */
    public void setSerializer(BinarySerializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Gets whether this is enum type.
     *
     * @return {@code True} if enum.
     */
    public boolean isEnum() {
        return isEnum;
    }

    /**
     * Sets whether this is enum type.
     *
     * @param isEnum {@code True} if enum.
     */
    public void setEnum(boolean isEnum) {
        this.isEnum = isEnum;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryTypeConfiguration.class, this, super.toString());
    }
}