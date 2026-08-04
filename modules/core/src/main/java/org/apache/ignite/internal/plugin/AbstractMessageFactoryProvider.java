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
import org.apache.ignite.internal.UseBinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactory;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMessageDeployer;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.NonMarshallableMessage;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link MessageFactoryProvider} that registers messages together with the companions codegen made for them, and
 * hands a {@link MarshallableMessage} the marshaller its class asks for: schema-aware when the class is annotated
 * with {@link UseBinaryMarshaller}, the default one otherwise.
 */
public abstract class AbstractMessageFactoryProvider implements MessageFactoryProvider {
    /** Generated-companion constructors per message class, indexed by {@link Companion#ordinal()}. */
    private static final ClassValue<Constructor<?>[]> COMPANIONS = new ClassValue<>() {
        @Override protected Constructor<?>[] computeValue(Class<?> cls) {
            Constructor<?>[] ctors = new Constructor<?>[Companion.values().length];

            for (Companion companion : Companion.values())
                ctors[companion.ordinal()] = companionCtor(cls, companion);

            return ctors;
        }
    };

    /** Default schema-less marshaller. */
    protected Marshaller dfltMarsh;

    /** Schema-aware marshaller like {@link BinaryMarshaller}. */
    protected Marshaller schemaAwareMarsh;

    /**
     * @param dfltMarsh Default schema-less marshaller like {@link JdkMarshaller}.
     * @param schemaAwareMarsh Schema-aware marshaller like {@link BinaryMarshaller}.
     */
    public void init(Marshaller dfltMarsh, Marshaller schemaAwareMarsh) {
        this.dfltMarsh = dfltMarsh;
        this.schemaAwareMarsh = schemaAwareMarsh;
    }

    /** Register a message with a caller-provided {@code id}. */
    protected <T extends Message> void register(IgniteMessageFactory factory, Class<T> cls, short id) {
        register(factory, cls, id, cls.getAnnotation(UseBinaryMarshaller.class) != null ? schemaAwareMarsh : dfltMarsh);
    }

    /** */
    private static <T extends Message> void register(IgniteMessageFactory factory, Class<T> cls, short id, Marshaller marsh) {
        MessageSerializer<T> serializer = loadGenerated(cls, Companion.SERIALIZER, null);

        MessageMarshaller<T> marshaller = NonMarshallableMessage.class.isAssignableFrom(cls)
            ? null
            : loadGenerated(cls, Companion.MARSHALLER, marsh);

        // Deployers exist for GridCacheMessage only; a DeployableMessage left without one is rejected at registration.
        GridCacheMessageDeployer<?> deployer = GridCacheMessage.class.isAssignableFrom(cls)
            ? loadGenerated(cls, Companion.DEPLOYER, null)
            : null;

        factory.register(id, serializer, marshaller, deployer);
    }

    /**
     * Instantiates the generated companion of {@code cls}. Only the marshaller may take a {@code Marshaller}, so
     * {@code marsh} is {@code null} for the other two. Lookups are cached in {@link #COMPANIONS}.
     *
     * @return the companion, or {@code null} when it is not generated and not required.
     */
    @SuppressWarnings("unchecked")
    private static <T> @Nullable T loadGenerated(Class<?> cls, Companion companion, @Nullable Marshaller marsh) {
        Constructor<?> ctor = COMPANIONS.get(cls)[companion.ordinal()];

        if (ctor == null) {
            if (companion.required(cls)) {
                throw new IgniteException("No " + companion.className(cls) + " found for " + cls.getName() +
                    ". Either the class is not processed by codegen or the generated sources are stale," +
                    " try 'mvn clean install'.");
            }

            return null;
        }

        assert ctor.getParameterCount() == 0 || marsh != null :
            companion.className(cls) + " takes a marshaller, but none was provided";

        try {
            return (T)(ctor.getParameterCount() == 0 ? ctor.newInstance() : ctor.newInstance(marsh));
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate " + companion.className(cls), e);
        }
    }

    /** @return the sole public constructor of the generated companion, or {@code null} when it does not exist. */
    private static @Nullable Constructor<?> companionCtor(Class<?> cls, Companion companion) {
        try {
            // The companion lives next to the message class, so it must be looked up in the same class loader.
            return Class.forName(cls.getName() + companion.suffix, true, cls.getClassLoader()).getConstructors()[0];
        }
        catch (ClassNotFoundException ignored) {
            return null;
        }
    }

    /** Companion class codegen generates next to a message. */
    private enum Companion {
        /** Reads and writes the message fields. Generated for every message. */
        SERIALIZER("Serializer"),

        /**
         * Takes the fields to their wire shape and back. Generated when the message has any wire work: a step of
         * its own, {@code @Marshalled} fields or fields to walk.
         */
        MARSHALLER("Marshaller"),

        /** Prepares the deployable fields. Generated for a {@link GridCacheMessage} that has them. */
        DEPLOYER("Deployer");

        /** Suffix the generated class name ends with. */
        private final String suffix;

        /** */
        Companion(String suffix) {
            this.suffix = suffix;
        }

        /**
         * A serializer exists for every message, and a {@link MarshallableMessage} always gets a wire — its own
         * {@code marshal} call alone is enough to generate one — so a missing companion in these two cases means a
         * stale build. The rest are generated only when the message gives them something to do.
         *
         * @return {@code true} if {@code cls} must have this companion.
         */
        boolean required(Class<?> cls) {
            return this == SERIALIZER || (this == MARSHALLER && MarshallableMessage.class.isAssignableFrom(cls));
        }

        /** @return the companion class name to report {@code cls} problems with. */
        String className(Class<?> cls) {
            return cls.getSimpleName() + suffix;
        }
    }
}
