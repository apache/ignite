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

import java.util.Collection;
import org.apache.ignite.internal.network.NetworkMessageTypes;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.Serialization;
import org.apache.ignite.internal.network.serialization.SerializationType;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/** Message for the {@link ClassDescriptor}. */
@Transferable(NetworkMessageTypes.CLASS_DESCRIPTOR_MESSAGE)
public interface ClassDescriptorMessage extends NetworkMessage {
    /**
     * Class name.
     *
     * @see ClassDescriptor#className()
     */
    String className();

    /**
     * Descriptor id.
     *
     * @see ClassDescriptor#descriptorId()
     */
    int descriptorId();

    /**
     * Super-class name.
     *
     * @see ClassDescriptor#superClassDescriptor()
     */
    @Nullable
    String superClassName();

    /**
     * Super-class descriptor ID. {@link Integer#MIN_VALUE} if super-class is missing.
     *
     * @see ClassDescriptor#superClassDescriptor()
     */
    int superClassDescriptorId();

    /**
     * List of the class fields' descriptors.
     *
     * @see ClassDescriptor#fields()
     */
    Collection<FieldDescriptorMessage> fields();

    /**
     * The type of the serialization mechanism for the class.
     *
     * @see SerializationType#value()
     */
    int serializationType();

    /**
     * Has serialization override.
     *
     * @see Serialization#hasSerializationOverride()
     */
    boolean hasSerializationOverride();

    /**
     * Has readObjectNoData().
     *
     * @see Serialization#hasReadObjectNoData()
     */
    boolean hasReadObjectNoData();

    /**
     * Has writeReplace.
     *
     * @see Serialization#hasWriteReplace()
     */
    boolean hasWriteReplace();

    /**
     * Has readResolve.
     *
     * @see Serialization#hasReadResolve()
     */
    boolean hasReadResolve();

    /**
     * Whether the class is final.
     *
     * @see ClassDescriptor#isFinal()
     */
    boolean isFinal();
}
