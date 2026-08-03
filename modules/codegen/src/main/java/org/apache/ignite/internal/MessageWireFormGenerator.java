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

package org.apache.ignite.internal;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.VariableElement;
import javax.tools.Diagnostic;

import static org.apache.ignite.internal.MessageProcessor.IGNITE_CHECKED_EXCEPTION_CLS;

/**
 * Generates the {@code *WireForm} class of a {@code Message}: the code that walks into its nested messages and
 * prepares its cache objects, so the fields are in the shape they go on the wire in. Marshalling a field is a step of
 * its own and belongs to {@link MessageMarshallerGenerator}; a message with nothing to walk gets no wire form.
 */
public class MessageWireFormGenerator extends MessageWireCompanionGenerator {
    /** Interface the generated wire forms implement. */
    private static final String MESSAGE_WIRE_FORM_CLS = "org.apache.ignite.plugin.extensions.communication.MessageWireForm";

    /** */
    MessageWireFormGenerator(ProcessingEnvironment env) {
        super(env);
    }

    /** {@inheritDoc} */
    @Override protected String typeSuffix() {
        return "WireForm";
    }

    /** {@inheritDoc} */
    @Override protected void generateBody(List<VariableElement> fields) {
        readFields();

        generateToWireMethod(fields);
        generateFromWireMethods(fields);
    }

    /** {@inheritDoc} */
    @Override protected String buildClassCode(String clsName) throws IOException {
        if (!hasStatements)
            return null;

        try (Writer writer = new StringWriter()) {
            imports.add(type.toString());
            imports.add(MESSAGE_WIRE_FORM_CLS);

            writeClassHeader(writer, "MessageWireForm", clsName);

            writer.write(" {" + NL);

            for (String line : methods)
                writer.write(line + NL);

            writer.write("}");

            return writer.toString();
        }
    }

    /** Generates the {@code toWire} method: every field on the way out. */
    private void generateToWireMethod(List<VariableElement> orderedFields) {
        imports.add(IGNITE_CHECKED_EXCEPTION_CLS);
        imports.add(GRID_KERNAL_CONTEXT_CLS);
        imports.add(CACHE_OBJECT_CONTEXT_CLS);

        String signature = "toWire(" + simpleNameWithGeneric(type) + " msg, GridKernalContext kctx, CacheObjectContext cacheObjCtx)";

        hasStatements |= emitMethod(methods, signature, body -> {
            usesMsgFactory = false;

            List<String> code = new ArrayList<>();

            appendFields(code, orderedFields, Direction.OUT);

            if (code.isEmpty())
                return;

            if (needsCtx(orderedFields))
                appendBlock(body, List.of(ctxResolutionLine()));

            body.addAll(code);

            prependMsgFactoryResolution(body);
        });
    }

    /** Generates the {@code fromWire} overloads: the NIO-eligible fields apart from the rest. */
    private void generateFromWireMethods(List<VariableElement> orderedFields) {
        List<VariableElement> nioFields = new ArrayList<>();
        List<VariableElement> workerFields = new ArrayList<>();

        for (VariableElement f : orderedFields) {
            boolean nioField = isNioField(f);

            if (nioField && isMessage(f.asType()) && !nestedNeedsCtx(f.asType()))
                nioFields.add(f);
            else {
                if (nioField && !isMessage(f.asType())) {
                    env.getMessager().printMessage(Diagnostic.Kind.ERROR,
                        "@NioField has no effect on non-Message field '" + f.getSimpleName() + "' of type " + f.asType(), f);
                }
                else if (nioField) {
                    env.getMessager().printMessage(Diagnostic.Kind.ERROR,
                        "@NioField field '" + f.getSimpleName() + "' of type " + f.asType() + " needs a cache object " +
                            "context to unmarshal, but the NIO thread has none; only context-free messages may be @NioField", f);
                }

                workerFields.add(f);
            }
        }

        String msgParam = simpleNameWithGeneric(type) + " msg, GridKernalContext kctx";

        generateFromWireMethod(msgParam + ", CacheObjectContext cacheObjCtx, ClassLoader clsLdr", workerFields);

        if (!nioFields.isEmpty())
            generateFromWireNioMethod(msgParam, nioFields);
    }

    /** Generates the cache-aware {@code fromWire} overload: the full field set, with cache context and class loader. */
    private void generateFromWireMethod(String params, List<VariableElement> fields) {
        hasStatements |= emitMethod(methods, "fromWire(" + params + ")", body -> {
            usesMsgFactory = false;

            Set<String> wireFieldSkip = marshalledWireFieldsToSkip();

            List<String> code = new ArrayList<>();

            appendFields(code, fields, Direction.IN, wireFieldSkip);

            if (code.isEmpty())
                return;

            if (needsCtx(fields))
                appendBlock(body, List.of(ctxResolutionLine()));

            body.addAll(code);

            prependMsgFactoryResolution(body);
        });
    }

    /** Generates the {@code fromWireNio} method for NIO-eligible {@code @Message} fields. */
    private void generateFromWireNioMethod(String params, List<VariableElement> nioFields) {
        hasStatements |= emitMethod(methods, "fromWireNio(" + params + ")", body -> {
            for (VariableElement f : nioFields)
                appendBlock(body, fromWireNioField(fieldAccessor(f)));
        });
    }

    /** Cache-free unmarshal of a {@code @NioField} message field on the NIO thread (no cache context available). */
    private List<String> fromWireNioField(String accessor) {
        imports.add(MESSAGE_WIRE_CLS);

        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null)", accessor));

        indent++;

        code.add(indentedLine("MessageWire.fromWire(%s, kctx);", accessor));

        indent--;

        return code;
    }
}
