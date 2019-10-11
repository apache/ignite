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

/**
 * Interface that allows to implement custom serialization logic for binary objects.
 * Can be used instead of {@link Binarylizable} in case if the class
 * cannot be changed directly.
 * <p>
 * Binary serializer can be configured for all binary objects via
 * {@link org.apache.ignite.configuration.BinaryConfiguration#getSerializer()} method, or for a specific
 * binary type via {@link BinaryTypeConfiguration#getSerializer()} method.
 */
public interface BinarySerializer {
    /**
     * Writes fields to provided writer.
     *
     * @param obj Empty object.
     * @param writer Binary object writer.
     * @throws BinaryObjectException In case of error.
     */
    public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException;

    /**
     * Reads fields from provided reader.
     *
     * @param obj Empty object
     * @param reader Binary object reader.
     * @throws BinaryObjectException In case of error.
     */
    public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException;
}
