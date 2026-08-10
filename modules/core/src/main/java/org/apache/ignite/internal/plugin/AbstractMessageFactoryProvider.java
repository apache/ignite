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
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.SelfMarshallingMessage;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactory;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMessageDeployer;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.NonMarshallableMessage;
import org.jetbrains.annotations.Nullable;

/**
 * An extension of {@link MessageFactoryProvider} that wires a message to its generated companions.
 */
public abstract class AbstractMessageFactoryProvider implements MessageFactoryProvider {
    /** Generated-companion constructors per message class, including cached negative lookups. */
    private static final ClassValue<Companions> COMPANIONS = new ClassValue<>() {
        @Override protected Companions computeValue(Class<?> cls) {
            return new Companions(companionCtor(cls, "Serializer"), companionCtor(cls, "Marshaller"), companionCtor(cls, "Deployer"));
        }
    };

    /** Register a message with a caller-provided {@code id}. */
    protected <T extends Message> void register(IgniteMessageFactory factory, Class<T> cls, short id) {
        MessageSerializer<T> serializer = loadGenerated(cls, "Serializer", true);

        // A message that marshals a part of its fields itself always gets a generated marshaller (its own call alone
        // is a statement), so its absence is a build problem. For the rest the generator skips statement-free
        // marshallers, so absence legitimately means "nothing to marshal"; the message and its companions ship in the
        // same jar, hence a missing class cannot be a packaging accident that spares the (required) serializer.
        MessageMarshaller<T> marshaller;

        if (NonMarshallableMessage.class.isAssignableFrom(cls))
            marshaller = null;
        else {
            boolean required = MarshallableMessage.class.isAssignableFrom(cls)
                || SelfMarshallingMessage.class.isAssignableFrom(cls);

            marshaller = loadGenerated(cls, "Marshaller", required);
        }

        // Deployers are generated for GridCacheMessage subclasses only, so the class lookup is skipped for the rest;
        // a DeployableMessage left without a deployer is then rejected at registration.
        GridCacheMessageDeployer<?> deployer = GridCacheMessage.class.isAssignableFrom(cls)
            ? loadGenerated(cls, "Deployer", false)
            : null;

        factory.register(id, serializer, marshaller, deployer);
    }

    /**
     * Instantiates the generated companion class {@code <message>Serializer/Marshaller/Deployer}. Only the marshaller
     * companion ever takes a {@code Marshaller}, and only when the message has fields to marshal with one, so
     * {@code marsh} is {@code null} for the other two. Constructor lookups, including missing companions, are cached
     * per message class in {@link #COMPANIONS}.
     *
     * @return the companion, or {@code null} when it is not generated and {@code required} is {@code false}.
     */
    @SuppressWarnings("unchecked")
    private static <T> @Nullable T loadGenerated(Class<?> cls, String suffix, boolean required) {
        Constructor<?> ctor = COMPANIONS.get(cls).ctor(suffix);

        if (ctor == null) {
            if (required) {
                throw new IgniteException("No " + cls.getSimpleName() + suffix + " found for " + cls.getName() +
                    ". Either the class is not processed by codegen or the generated sources are stale," +
                    " try 'mvn clean install'.");
            }

            return null;
        }

        try {
            return (T)ctor.newInstance();
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate " + cls.getSimpleName() + suffix, e);
        }
    }

    /** @return the sole public constructor of the generated companion {@code <message><suffix>}, or {@code null} when it does not exist. */
    private static @Nullable Constructor<?> companionCtor(Class<?> cls, String suffix) {
        try {
            // The companion lives next to the message class, so it must be looked up in the same class loader.
            return Class.forName(cls.getName() + suffix, true, cls.getClassLoader()).getConstructors()[0];
        }
        catch (ClassNotFoundException ignored) {
            return null;
        }
    }

    /** Generated-companion constructors of one message class; a {@code null} entry means the companion is not generated. */
    private static final class Companions {
        /** */
        private final @Nullable Constructor<?> serializer;

        /** */
        private final @Nullable Constructor<?> marshaller;

        /** */
        private final @Nullable Constructor<?> deployer;

        /** */
        Companions(@Nullable Constructor<?> serializer, @Nullable Constructor<?> marshaller, @Nullable Constructor<?> deployer) {
            this.serializer = serializer;
            this.marshaller = marshaller;
            this.deployer = deployer;
        }

        /** @return the constructor of the {@code suffix} companion, or {@code null} when it is not generated. */
        @Nullable Constructor<?> ctor(String suffix) {
            switch (suffix) {
                case "Serializer":
                    return serializer;

                case "Marshaller":
                    return marshaller;

                case "Deployer":
                    return deployer;

                default:
                    throw new IllegalArgumentException("Unknown companion suffix: " + suffix);
            }
        }
    }
}
