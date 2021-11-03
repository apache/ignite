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
import org.apache.ignite.network.serialization.MessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;

/**
 * Annotation that should be placed on classes or interfaces that will be treated as <i>message group descriptors</i>.
 *
 * <p>Such classes represent a group of messages, declared in a single module, and are used by the annotation processor to create
 * module-wide classes, such as message factories (one factory should serve as the only entry point for creating Network Message instances
 * declared in a single module) and serialization registry initializers (helper classes that register all generated
 * {@link MessageSerializationFactory} instances in a {@link MessageSerializationRegistry}). All module-wide generated classes will be
 * placed in the same package as their message group descriptors.
 *
 * <p>The content of these classes is not specified and can be left empty, though the convention is to use them as namespaces for declaring
 * types of all Network Messages in the module.
 *
 * @see Transferable
 */
@Target(ElementType.TYPE)
// using the RUNTIME retention policy in order to avoid problems with incremental compilation in an IDE.
@Retention(RetentionPolicy.RUNTIME)
public @interface MessageGroup {
    /**
     * Name of the message group.
     *
     * <p>Group names are used as part of the generated class names using the following patterns:
     *
     * <ol>
     *     <li>Message factories: {@code <GroupName>Factory}</li>
     *     <li>Serialization registry initializers: {@code <GroupName>SerializationRegistryInitializer}</li>
     * </ol>
     *
     * <p>Since the group name is interpreted as-is, it should follow the Java class naming convention.
     *
     * @return group name.
     */
    String groupName();

    /**
     * Type of the group.
     *
     * <p>Group type must be unique across all <i>message group descriptors</i>. Both group type and message type are used to identify a
     * concrete message implementation during serialization.
     *
     * @return group type.
     */
    short groupType();
}
