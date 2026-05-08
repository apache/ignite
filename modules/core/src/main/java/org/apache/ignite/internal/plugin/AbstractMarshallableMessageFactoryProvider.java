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
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;

/**
 * An extension of {@link MessageFactoryProvider} allowing to use provided schema-aware marshaller
 * and resolved class loader to register {@link MarshallableMessage}.
 */
public abstract class AbstractMarshallableMessageFactoryProvider implements MessageFactoryProvider {
    /** Default schema-less marshaller. */
    protected Marshaller dfltMarsh;

    /** Default class loader. */
    protected final ClassLoader dftlClsLdr = U.gridClassLoader();

    /** Schema-aware marshaller like {@link BinaryMarshaller}. */
    protected Marshaller schemaAwareMarsh;

    /** Resolved (configured) class loader like {@link IgniteConfiguration#setClassLoader(ClassLoader)}. */
    protected ClassLoader resolvedClsLdr;

    /**
     * @param dfltMarsh Default schema-less marshaller like {@link JdkMarshaller}.
     * @param schemaAwareMarsh Schema-aware marshaller like {@link BinaryMarshaller}.
     * @param resolvedClsLdr Resolved (configured) class loader like {@link IgniteConfiguration#setClassLoader(ClassLoader)}.
     */
    public void init(Marshaller dfltMarsh, Marshaller schemaAwareMarsh, ClassLoader resolvedClsLdr) {
        this.dfltMarsh = dfltMarsh;
        this.schemaAwareMarsh = schemaAwareMarsh;
        this.resolvedClsLdr = resolvedClsLdr;
    }

    /** Registers message automatically generating message supplier and serializer. */
    protected static <T extends Message> void register(MessageFactory factory, Class<T> cls, short id, Marshaller marsh, 
        ClassLoader clsLrd) {
        Constructor<T> ctor;
        MessageSerializer<T> serializer;

        try {
            ctor = cls.getConstructor();

            boolean marshallable = MarshallableMessage.class.isAssignableFrom(cls);

            Class<?> serCls = Class.forName(cls.getName() + (marshallable ? "MarshallableSerializer" : "Serializer"));

            serializer = marshallable
                ? (MessageSerializer<T>)serCls.getConstructor(Marshaller.class, ClassLoader.class).newInstance(marsh, clsLrd)
                : (MessageSerializer<T>)serCls.getConstructor().newInstance();
        }
        catch (Exception e) {
            throw new IgniteException("Failed to register message of type " + cls.getSimpleName(), e);
        }

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
            serializer
        );
    }
}
