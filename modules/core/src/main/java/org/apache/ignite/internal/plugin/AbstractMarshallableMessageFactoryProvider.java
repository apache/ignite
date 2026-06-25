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

package org.apache.ignite.internal.plugin;

import java.lang.reflect.Constructor;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.GridCacheMessageDeployer;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.NonMarshallableMessage;
import org.jetbrains.annotations.Nullable;

/**
 * An extension of {@link MessageFactoryProvider} allowing to use provided schema-aware marshaller
 * and resolved class loader to register {@link MarshallableMessage}.
 */
public abstract class AbstractMarshallableMessageFactoryProvider implements MessageFactoryProvider {
    /** Default schema-less marshaller. */
    protected Marshaller dfltMarsh;

    /** Schema-aware marshaller like {@link BinaryMarshaller}. */
    protected Marshaller schemaAwareMarsh;

    /**
     * @param dfltMarsh Default schema-less marshaller like {@link JdkMarshaller}.
     * @param schemaAwareMarsh Schema-aware marshaller like {@link BinaryMarshaller}.
     * @param resolvedClsLdr Resolved (configured) class loader like {@link IgniteConfiguration#setClassLoader(ClassLoader)}.
     */
    public void init(Marshaller dfltMarsh, Marshaller schemaAwareMarsh, ClassLoader resolvedClsLdr) {
        this.dfltMarsh = dfltMarsh;
        this.schemaAwareMarsh = schemaAwareMarsh;
    }

    /** Registers a message with its generated serializer, marshaller (if marshallable), and deployer (if any). */
    protected static <T extends Message> void register(MessageFactory factory, Class<T> cls, short id, Marshaller marsh) {
        Constructor<T> ctor;

        try {
            ctor = cls.getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw new IgniteException("Failed to register message of type " + cls.getSimpleName(), e);
        }

        MessageSerializer<T> serializer = loadGenerated(cls, "Serializer", marsh);

        MessageMarshaller<T> marshaller = NonMarshallableMessage.class.isAssignableFrom(cls)
            ? null
            : loadGenerated(cls, "Marshaller", marsh);

        GridCacheMessageDeployer deployer = loadGenerated(cls, "Deployer", marsh);

        factory.register(
            id,
            () -> {
                try {
                    return ctor.newInstance();
                }
                catch (Exception e) {
                    throw new IgniteException("Failed to create message of type " + cls.getSimpleName(), e);
                }
            },
            serializer,
            marshaller,
            deployer
        );
    }

    /**
     * Loads and instantiates the generated companion class {@code <message>Serializer/Marshaller/Deployer}, or returns
     * {@code null} when it does not exist. The sole declared constructor is used, passing {@code marsh} when it takes one.
     */
    @SuppressWarnings("unchecked")
    private static <T> @Nullable T loadGenerated(Class<?> cls, String suffix, Marshaller marsh) {
        Class<?> generated;

        try {
            generated = Class.forName(cls.getName() + suffix);
        }
        catch (ClassNotFoundException ignored) {
            return null;
        }

        try {
            Constructor<?> ctor = generated.getConstructors()[0];

            return (T)(ctor.getParameterCount() == 0 ? ctor.newInstance() : ctor.newInstance(marsh));
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate " + cls.getSimpleName() + suffix, e);
        }
    }
}
