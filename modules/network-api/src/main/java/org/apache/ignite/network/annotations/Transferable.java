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

package org.apache.ignite.network.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializer;

/**
 * Annotation for interfaces intended to be used as <i>Transferable Object</i>. Such objects are intended to be serialized and sent over the
 * network. The interfaces must obey the following contract:
 *
 * <ol>
 *     <li>They must only contain Ignite-style getter methods that represent the objects' properties.</li>
 *     <li>They must extend the {@link NetworkMessage} interface either directly or transitively. This
 *     requirement is subject to change in the future when nested Transferable Objects will be supported.</li>
 * </ol>
 *
 * <p>When such interface is marked by this annotation, it can be used by the annotation processor to generate
 * the following classes:
 *
 * <ol>
 *     <li>Builder interface with setters for all declared properties;</li>
 *     <li>An immutable implementation of the interface and a nested implementation of the generated builder
 *     for creating new instances;</li>
 * </ol>
 *
 * <p>If the {@link #autoSerializable} property is set to {@code true}, the annotation processor will additionally generate
 * serialization-related classes:
 *
 * <ol>
 *     <li>{@link MessageSerializer};</li>
 *     <li>{@link MessageDeserializer};</li>
 *     <li>{@link MessageSerializationFactory}.</li>
 * </ol>
 *
 * <p>Properties of Transferable Objects that can be auto-serialized can only be of <i>directly marshallable type</i>,
 * which is one of the following:
 *
 * <ol>
 *     <li>Primitive type;</li>
 *     <li>{@code String};</li>
 *     <li>{@link UUID};</li>
 *     <li>{@link IgniteUuid};</li>
 *     <li>{@link BitSet};</li>
 *     <li>Nested {@code NetworkMessage};</li>
 *     <li>Array of primitive types, corresponding boxed types or other directly marshallable types;</li>
 *     <li>{@code Collection} of boxed primitive types or other directly marshallable types;</li>
 *     <li>{@code Map} where both keys and values can be of a directly marshallable type.</li>
 * </ol>
 *
 * <p>After all marked interfaces in a module have been processed, the processor will use the
 * <i>message group descriptor</i> (class annotated with {@link MessageGroup}) to expose the builders via a
 * Message Factory.
 *
 * @see MessageGroup
 */
@Target(ElementType.TYPE)
// using the RUNTIME retention policy in order to avoid problems with incremental compilation in an IDE.
@Retention(RetentionPolicy.RUNTIME)
// TODO: Update this annotation according to https://issues.apache.org/jira/browse/IGNITE-14817
public @interface Transferable {
    /**
     * Returns this message's type as described in {@link NetworkMessage#messageType}.
     *
     * @return Message type.
     */
    short value();

    /**
     * When this property is set to {@code true} (default), serialization-related classes will be generated in addition to the message
     * implementation.
     *
     * @return {code true} if serialization classes need to be generated, {@code false} otherwise
     */
    boolean autoSerializable() default true;
}
