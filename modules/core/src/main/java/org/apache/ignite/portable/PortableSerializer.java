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

package org.apache.ignite.portable;

import org.apache.ignite.marshaller.portable.PortableMarshaller;

/**
 * Interface that allows to implement custom serialization logic for portable objects.
 * Can be used instead of {@link PortableMarshalAware} in case if the class
 * cannot be changed directly.
 * <p>
 * Portable serializer can be configured for all portable objects via
 * {@link PortableMarshaller#getSerializer()} method, or for a specific
 * portable type via {@link PortableTypeConfiguration#getSerializer()} method.
 */
public interface PortableSerializer {
    /**
     * Writes fields to provided writer.
     *
     * @param obj Empty object.
     * @param writer Portable object writer.
     * @throws PortableException In case of error.
     */
    public void writePortable(Object obj, PortableWriter writer) throws PortableException;

    /**
     * Reads fields from provided reader.
     *
     * @param obj Empty object
     * @param reader Portable object reader.
     * @throws PortableException In case of error.
     */
    public void readPortable(Object obj, PortableReader reader) throws PortableException;
}