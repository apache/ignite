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

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;

/**
 * Binary meta data handler.
 */
public interface BinaryMetadataHandler {
    /**
     * Adds meta data.
     *
     * @param typeId Type ID.
     * @param meta Metadata.
     * @throws BinaryObjectException In case of error.
     */
    public void addMeta(int typeId, BinaryType meta) throws BinaryObjectException;

    /**
     * Gets meta data for provided type ID.
     *
     * @param typeId Type ID.
     * @return Metadata.
     * @throws BinaryObjectException In case of error.
     */
    public BinaryType metadata(int typeId) throws BinaryObjectException;

    /**
     * Gets unwrapped meta data for provided type ID.
     *
     * @param typeId Type ID.
     * @return Metadata.
     * @throws BinaryObjectException In case of error.
     */
    public BinaryMetadata metadata0(int typeId) throws BinaryObjectException;

    /**
     * Gets metadata for provided type ID and schema ID.
     *
     * @param typeId Type ID.
     * @param schemaId Schema ID.
     * @return Metadata.
     * @throws BinaryObjectException In case of error.
     */
    public BinaryType metadata(int typeId, int schemaId) throws BinaryObjectException;
}
