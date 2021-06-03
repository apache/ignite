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

package org.apache.ignite.network.processor.internal.messages;

import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.network.processor.internal.MessageGroupWrapper;
import org.apache.ignite.network.processor.internal.MessageClass;

/**
 * Class for generating factories for Network Messages inside the given module.
 */
public class MessageFactoryGenerator {
    /** */
    private final ProcessingEnvironment processingEnvironment;

    /** Message group. */
    private final MessageGroupWrapper messageGroup;

    /** */
    public MessageFactoryGenerator(
        ProcessingEnvironment processingEnvironment,
        MessageGroupWrapper messageGroup
    ) {
        this.processingEnvironment = processingEnvironment;
        this.messageGroup = messageGroup;
    }

    /**
     * Generates a factory for all Network Messages inside the given module.
     */
    public TypeSpec generateMessageFactory(List<MessageClass> messages) {
        ClassName factoryName = messageGroup.messageFactoryClassName();

        processingEnvironment.getMessager().printMessage(Diagnostic.Kind.NOTE, "Generating " + factoryName);

        TypeSpec.Builder messageFactory = TypeSpec.classBuilder(factoryName)
            .addModifiers(Modifier.PUBLIC)
            .addOriginatingElement(messageGroup.element());

        for (MessageClass message : messages) {
            MethodSpec buildMethod = MethodSpec.methodBuilder(message.asMethodName())
                .addModifiers(Modifier.PUBLIC)
                .returns(message.builderClassName())
                .addStatement("return $T.builder()", message.implClassName())
                .build();

            messageFactory
                .addMethod(buildMethod)
                .addOriginatingElement(message.element());
        }

        return messageFactory.build();
    }
}
