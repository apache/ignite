/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.ap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

/**
 * An annotation processor for testing.
 */
public class TestAnnotationProcessor extends AbstractProcessor {

    /**
     * The message key.
     */
    public static final String MESSAGES_KEY =
            TestAnnotationProcessor.class.getName() + "-messages";

    @Override
    public Set<String> getSupportedAnnotationTypes() {

        for (OutputMessage outputMessage : findMessages()) {
            processingEnv.getMessager().printMessage(outputMessage.kind, outputMessage.message);
        }

        return Collections.emptySet();
    }

    private static List<OutputMessage> findMessages() {
        final String messagesStr = System.getProperty(MESSAGES_KEY);
        if (messagesStr == null || messagesStr.isEmpty()) {
            return Collections.emptyList();
        }
        List<OutputMessage> outputMessages = new ArrayList<>();

        for (String msg : messagesStr.split("\\|")) {
            String[] split = msg.split(",");
            if (split.length == 2) {
                outputMessages.add(new OutputMessage(Diagnostic.Kind.valueOf(split[0]), split[1]));
            } else {
                throw new IllegalStateException(
                        "Unable to parse messages definition for: '" +
                                messagesStr + "'");
            }
        }

        return outputMessages;
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        return false;
    }

    /**
     * An output message.
     */
    private static class OutputMessage {
        public final Diagnostic.Kind kind;
        public final String message;

        OutputMessage(Diagnostic.Kind kind, String message) {
            this.kind = kind;
            this.message = message;
        }
    }
}
