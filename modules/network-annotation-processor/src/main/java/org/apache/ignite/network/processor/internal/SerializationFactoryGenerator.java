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

package org.apache.ignite.network.processor.internal;

import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializer;

/**
 * Class for generating {@link MessageSerializationFactory} classes.
 */
class SerializationFactoryGenerator {
    /**
     * Element representing a network message type declaration
     */
    private final ClassName messageClassName;

    /** */
    SerializationFactoryGenerator(TypeElement messageClass) {
        messageClassName = ClassName.get(messageClass);
    }

    /**
     * Generates a {@link MessageSerializationFactory} class for the given network message type.
     */
    TypeSpec generateFactory(TypeSpec serializer, TypeSpec deserializer) {
        return TypeSpec.classBuilder(messageClassName.simpleName() + "SerializationFactory")
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(ParameterizedTypeName.get(ClassName.get(MessageSerializationFactory.class), messageClassName))
            .addMethod(
                MethodSpec.methodBuilder("createDeserializer")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(MessageDeserializer.class), messageClassName))
                    .addStatement("return new $N()", deserializer)
                    .build()
            )
            .addMethod(
                MethodSpec.methodBuilder("createSerializer")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(MessageSerializer.class), messageClassName))
                    .addStatement("return new $N()", serializer)
                    .build()
            )
            .build();
    }
}
