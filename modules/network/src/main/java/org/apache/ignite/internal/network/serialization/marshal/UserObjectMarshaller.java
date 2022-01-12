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

package org.apache.ignite.internal.network.serialization.marshal;

import java.lang.reflect.Field;
import org.apache.ignite.internal.network.serialization.IdIndexedDescriptors;
import org.jetbrains.annotations.Nullable;

/**
 * Marshals/unmarshals objects in accordance with User Object Serialization.
 *
 * @see <a href="https://github.com/gridgain/gridgain-9-ce/blob/iep-67/modules/network/README.md">IEP-67</a>
 */
public interface UserObjectMarshaller {
    /**
     * Marshals the provided object.
     *
     * @param object        object to marshal
     * @param declaredClass class of the object as it is perceived externally, usually in {@link Field#getType()} from which the value
     *                      is extracted; it may differ from object.getClass() only when it is for a primitive type (i.e. byte.class)
     * @return marshalled representation
     * @throws MarshalException if marshalling fails
     */
    MarshalledObject marshal(@Nullable Object object, Class<?> declaredClass) throws MarshalException;

    /**
     * Unmarshals an object.
     *
     * @param bytes             bytes representing the marshalled object
     * @param mergedDescriptors the remote descriptors that need to be used for unmarshalling plus our local descriptors
     *                          (remote ones have the priority)
     * @param <T> expected type
     * @return unmarshalled object
     * @throws UnmarshalException if unmarshalling fails
     */
    @Nullable
    <T> T unmarshal(byte[] bytes, IdIndexedDescriptors mergedDescriptors) throws UnmarshalException;
}
