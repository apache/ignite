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

import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;

/**
 * Marshals objects to a {@link DataOutput} and also tracks what {@link ClassDescriptor}s were used when marshalling.
 */
interface TrackingMarshaller {
    /**
     * Marshals the given object to the {@link DataOutput}.
     *
     * @param object    object to marshal
     * @param output    where to marshal to
     * @return {@link ClassDescriptor}s that were used when marshalling
     * @throws IOException      if an I/O problem occurs
     * @throws MarshalException if another problem occurs
     */
    Set<ClassDescriptor> marshal(Object object, DataOutput output) throws IOException, MarshalException;
}
