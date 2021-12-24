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

package org.apache.ignite.internal.network.message;

import org.apache.ignite.internal.network.NetworkMessageTypes;
import org.apache.ignite.internal.network.serialization.FieldDescriptor;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/** Message for the {@link FieldDescriptor}. */
@Transferable(NetworkMessageTypes.FIELD_DESCRIPTOR_MESSAGE)
public interface FieldDescriptorMessage extends NetworkMessage {
    /**
     * Name of the field.
     */
    String name();

    /**
     * Field type's descriptor id.
     */
    int typeDescriptorId();

    /**
     * Field's class name.
     */
    String className();
}
