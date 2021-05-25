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

package org.apache.ignite.network.processor.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializer;

/**
 * Annotation for marking network message interfaces (i.e. those extending the {@link NetworkMessage} interface). For
 * such interfaces, the following classes will be generated:
 *
 * <ol>
 *     <li>{@link MessageSerializer};</li>
 *     <li>{@link MessageDeserializer};</li>
 *     <li>{@link MessageSerializationFactory}.</li>
 * </ol>
 * <p>
 * These messages must obey the <i>network message declaration contract</i> and can only contain
 * <i>directly marshallable types</i>, which can be one of the following:
 *
 * <ol>
 *     <li>Primitive type;</li>
 *     <li>{@link String};</li>
 *     <li>{@link UUID};</li>
 *     <li>{@link IgniteUuid};</li>
 *     <li>{@link BitSet};</li>
 *     <li>Nested {@link NetworkMessage};</li>
 *     <li>Array of primitive types, corresponding boxed types or other directly marshallable types;</li>
 *     <li>{@link Collection} of boxed primitive types or other directly marshallable types;</li>
 *     <li>{@link Map} where both keys and values can be of a directly marshallable type.</li>
 * </ol>
 */
// TODO: describe the message declaration contract, see https://issues.apache.org/jira/browse/IGNITE-14715
@Target(ElementType.TYPE)
// using the RUNTIME retention policy in order to avoid problems with incremental compilation in an IDE.
@Retention(RetentionPolicy.RUNTIME)
public @interface AutoSerializable {
    /**
     * Message factory class that will be used to create message builders during deserialization.
     * <p>
     * Message factories must have a static method with the same name as the created message type and return a builder
     * for that type, e.g. a factory for creating {@code TestMessage} instances must have a {@code TestMessage.Builder
     * testMessage()} method.
     */
    Class<?> messageFactory();
}
