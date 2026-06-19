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
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.QualifiedNameable;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;

import static org.apache.ignite.internal.MessageProcessor.MARSHALLABLE_MESSAGE_INTERFACE;
import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;

/**
 * Generates marshaller class for a given {@code Message} class. The generated marshaller follows the naming convention:
 * {@code org.apache.ignite.internal.codegen.[MessageClassName]Marshaller}.
 * <p>
 * No marshaller is generated for {@code NonMarshallableMessage} types.
 */
public class MessageMarshallerGenerator extends MessageGenerator {
    /** Collection of lines for {@code prepareMarshal} / {@code finishUnmarshal} methods. */
    private final List<String> marshall = new ArrayList<>();

    /** The marshallable message type. */
    private final TypeMirror marshallableMsgType;

    /** The cache-marshallable message type. */
    private final TypeMirror cacheMarshallableMsgType;

    /** */
    MessageMarshallerGenerator(ProcessingEnvironment env) {
        super(env);

        TypeElement marshallableMsgElem = env.getElementUtils().getTypeElement(MARSHALLABLE_MESSAGE_INTERFACE);
        marshallableMsgType = marshallableMsgElem != null ? marshallableMsgElem.asType() : null;

        TypeElement cacheMarshallableMsgElem = env.getElementUtils()
            .getTypeElement("org.apache.ignite.plugin.extensions.communication.CacheMarshallableMessage");
        cacheMarshallableMsgType = cacheMarshallableMsgElem != null ? cacheMarshallableMsgElem.asType() : null;
    }

    /** {@inheritDoc} */
    @Override String typeSuffix() {
        return "Marshaller";
    }

    /** {@inheritDoc} */
    @Override boolean shouldSkip(TypeElement type) {
        return isNonMarshallableMessage(type);
    }

    /** {@inheritDoc} */
    @Override void generateBody(List<VariableElement> fields) throws Exception {
        generateMarshallMethods(fields);
        generateUnmarshallMethods(fields);
    }

    /** {@inheritDoc} */
    @Override String buildClassCode(String marshallerClsName) throws IOException {
        try (Writer writer = new StringWriter()) {
            imports.add(type.toString());
            imports.add("org.apache.ignite.plugin.extensions.communication.MessageMarshaller");

            if (marshallableMessage())
                imports.add("org.apache.ignite.marshaller.Marshaller");

            writeClassHeader(writer, "MessageMarshaller", marshallerClsName);

            writeConstructor(writer, marshallerClsName);

            for (String line : marshall)
                writer.write(line + NL);

            writer.write("}");

            return writer.toString();
        }
    }

    /** */
    private void writeConstructor(Writer writer, String marshallerClsName) throws IOException {
        indent = 1;

        writer.write(indentedLine(METHOD_JAVADOC));
        writer.write(NL);

        if (marshallableMessage()) {
            writer.write(indentedLine("private final Marshaller marshaller;"));
            writer.write(NL + NL);

            writer.write(indentedLine(METHOD_JAVADOC));
            writer.write(NL);
            writer.write(indentedLine("public " + marshallerClsName + "(Marshaller marshaller) {"));
            writer.write(NL);

            indent++;
            writer.write(indentedLine("this.marshaller = marshaller;"));
            writer.write(NL);
            indent--;
        }
        else {
            writer.write(indentedLine("public " + marshallerClsName + "() {"));
            writer.write(NL);
        }

        writer.write(NL);
        writer.write(indentedLine("}"));
        writer.write(NL + NL);
    }

    /** */
    private void generateMarshallMethods(List<VariableElement> orderedFields) {
        imports.add("org.apache.ignite.IgniteCheckedException");
        imports.add("org.apache.ignite.internal.GridKernalContext");
        imports.add("org.apache.ignite.internal.processors.cache.GridCacheContext");

        indent = 1;

        marshall.add(indentedLine(METHOD_JAVADOC));

        marshall.add(indentedLine(
            "@Override public void prepareMarshal(" + simpleNameWithGeneric(type) +
                " msg, GridKernalContext kctx, GridCacheContext<?, ?> nested) throws IgniteCheckedException {"));

        indent++;

        int prepareBodyStart = marshall.size();

        if (needsCtx(orderedFields))
            marshall.add(ctxResolutionLine());

        if (marshallableMessage()) {
            if (marshall.size() > prepareBodyStart)
                marshall.add(EMPTY);
            marshall.add(indentedLine("msg.prepareMarshal(marshaller);"));
        }

        for (VariableElement field : orderedFields) {
            List<String> marshalled = marshall(field.asType(), fieldAccessor(field), MarshalMode.PREPARE);

            if (!marshalled.isEmpty()) {
                if (!marshall.get(marshall.size() - 1).equals(EMPTY))
                    marshall.add(EMPTY);

                marshall.addAll(marshalled);
            }
        }

        indent--;

        marshall.add(indentedLine("}"));
    }

    /** */
    private void generateUnmarshallMethods(List<VariableElement> orderedFields) {
        marshall.add(EMPTY);

        indent = 1;

        marshall.add(indentedLine(METHOD_JAVADOC));

        marshall.add(indentedLine(
            "@Override public void finishUnmarshal(" + simpleNameWithGeneric(type) +
                " msg, GridKernalContext kctx, GridCacheContext<?, ?> nested, ClassLoader clsLdr) throws IgniteCheckedException {"));

        indent++;

        int finish5BodyStart = marshall.size();

        if (needsCtx(orderedFields))
            marshall.add(ctxResolutionLine());

        for (VariableElement field : orderedFields) {
            List<String> unmarshalled = marshall(field.asType(), fieldAccessor(field), MarshalMode.FINISH_CACHE);

            if (!unmarshalled.isEmpty()) {
                if (!marshall.get(marshall.size() - 1).equals(EMPTY))
                    marshall.add(EMPTY);

                marshall.addAll(unmarshalled);
            }
        }

        if (isCacheMarshallableMessage(type)) {
            if (marshall.size() > finish5BodyStart)
                marshall.add(EMPTY);
            marshall.add(indentedLine("msg.finishUnmarshal(marshaller, clsLdr);"));
        }

        indent--;

        marshall.add(indentedLine("}"));

        imports.add("org.apache.ignite.internal.util.typedef.internal.U");

        marshall.add(EMPTY);

        marshall.add(indentedLine(METHOD_JAVADOC));

        marshall.add(indentedLine(
            "@Override public void finishUnmarshal(" + simpleNameWithGeneric(type) +
                " msg, GridKernalContext kctx) throws IgniteCheckedException {"));

        indent++;

        boolean first = true;

        for (VariableElement field : orderedFields) {
            List<String> unmarshalled = marshall(field.asType(), fieldAccessor(field), MarshalMode.FINISH_BASE);

            if (!unmarshalled.isEmpty()) {
                if (!first)
                    marshall.add(EMPTY);

                first = false;

                marshall.addAll(unmarshalled);
            }
        }

        if (marshallableMessage() && !isCacheMarshallableMessage(type)) {
            if (!first)
                marshall.add(EMPTY);

            marshall.add(indentedLine("msg.finishUnmarshal(marshaller, U.resolveClassLoader(kctx.config()));"));
        }

        indent--;

        marshall.add(indentedLine("}"));
    }

    /** Returns the {@code GridCacheContext ctx} resolution line for the current message type. */
    private String ctxResolutionLine() {
        if (isCacheIdAwareMessage(type))
            return indentedLine("GridCacheContext<?, ?> ctx = nested == null ? " +
                    "kctx.cache().context().cacheContext(msg.cacheId()) : nested;");
        else if (isCacheGroupIdMessage(type))
            return indentedLine("GridCacheContext<?, ?> ctx = nested == null ? " +
                    "kctx.cache().context().cacheContext(msg.groupId()) : nested;");
        else
            return indentedLine("GridCacheContext<?, ?> ctx = nested;");
    }

    /** Returns {@code true} if any field requires {@code ctx} in generated marshal/unmarshal code. */
    private boolean needsCtx(List<VariableElement> fields) {
        return fields.stream().anyMatch(f -> needsCtxType(f.asType()));
    }

    /** */
    private boolean needsCtxType(TypeMirror t) {
        if (t.getKind() == TypeKind.ARRAY)
            return needsCtxType(((ArrayType)t).getComponentType());

        if (t.getKind() == TypeKind.DECLARED || t.getKind() == TypeKind.TYPEVAR) {
            if (isMessage(t) || isCacheObject(t))
                return true;

            if (assignableFrom(erasedType(t), type(java.util.Map.class.getName()))) {
                List<? extends TypeMirror> args = ((DeclaredType)t).getTypeArguments();
                return needsCtxType(args.get(0)) || needsCtxType(args.get(1));
            }

            if (assignableFrom(erasedType(t), type(java.util.Collection.class.getName()))) {
                List<? extends TypeMirror> args = ((DeclaredType)t).getTypeArguments();
                return needsCtxType(args.get(0));
            }
        }

        return false;
    }

    /**
     * Describes which marshalling operation to generate code for.
     *
     * <ul>
     *   <li>{@link #PREPARE} — {@code prepareMarshal}: marshal CacheObjects and recurse into nested Messages.</li>
     *   <li>{@link #FINISH_BASE} — {@code finishUnmarshal(3-arg)}: lightweight unmarshal; recurses into nested
     *   Messages only, CacheObject fields are skipped (no cache context available).</li>
     *   <li>{@link #FINISH_CACHE} — {@code finishUnmarshal(5-arg)}: unmarshal with full cache context and class loader.</li>
     * </ul>
     */
    private enum MarshalMode {
        PREPARE,
        FINISH_BASE,
        FINISH_CACHE
    }

    /** */
    private List<String> marshall(TypeMirror t, String accessor, MarshalMode mode) {
        if (t.getKind() == TypeKind.ARRAY) {
            TypeMirror comp = ((ArrayType)t).getComponentType();

            if (comp.getKind() == TypeKind.DECLARED) {
                List<String> code = new ArrayList<>();

                imports.add(((QualifiedNameable)((DeclaredType)comp).asElement()).getQualifiedName().toString());

                code.add(indentedLine("if (%s != null) {", accessor));

                indent++;

                String el = "e" + indent;

                code.add(indentedLine("for (%s %s : %s) {", ((DeclaredType)comp).asElement().getSimpleName().toString(), el, accessor));

                indent++;

                List<String> res = marshall(comp, el, mode);

                code.addAll(res);

                indent--;

                code.add(indentedLine("}"));

                indent--;

                code.add(indentedLine("}"));

                if (res.isEmpty())
                    return java.util.Collections.emptyList();
                else
                    return code;
            }
        }
        else if (t.getKind() == TypeKind.DECLARED || t.getKind() == TypeKind.TYPEVAR) {
            if (isMessage(t)) {
                List<String> code = new ArrayList<>();

                code.add(indentedLine("if (%s != null)", accessor));

                indent++;

                switch (mode) {
                    case PREPARE:
                        code.add(indentedLine(
                            "MessageMarshaller.prepareMarshal(kctx.messageFactory(), %s, kctx, ctx);", accessor));
                        break;
                    case FINISH_BASE:
                        code.add(indentedLine(
                            "MessageMarshaller.finishUnmarshal(kctx.messageFactory(), %s, kctx);", accessor));
                        break;
                    case FINISH_CACHE:
                        code.add(indentedLine(
                            "MessageMarshaller.finishUnmarshal(kctx.messageFactory(), %s, kctx, ctx, clsLdr);", accessor));
                        break;
                }

                indent--;

                return code;
            }
            else if (isCacheObject(t)) {
                if (mode == MarshalMode.FINISH_BASE)
                    return java.util.Collections.emptyList();

                List<String> code = new ArrayList<>();

                code.add(indentedLine("if (%s != null && ctx != null)", accessor));

                indent++;

                if (mode == MarshalMode.PREPARE)
                    code.add(indentedLine("%s.prepareMarshal(ctx.cacheObjectContext());", accessor));
                else
                    code.add(indentedLine("%s.finishUnmarshal(ctx.cacheObjectContext(), clsLdr);", accessor));

                indent--;

                return code;
            }
            else if (assignableFrom(erasedType(t), type(java.util.Map.class.getName()))) {
                List<? extends TypeMirror> args = ((DeclaredType)t).getTypeArguments();

                TypeMirror keyType = args.get(0);
                TypeMirror valType = args.get(1);

                List<String> code = new ArrayList<>();

                code.add(indentedLine("if (%s != null) {", accessor));

                indent++;

                String el = "e" + indent;

                indent++;
                List<String> keyRes = marshall(keyType, el, mode);
                List<String> valRes = marshall(valType, el, mode);
                indent--;

                if (!keyRes.isEmpty() && (keyType.getKind() == TypeKind.DECLARED || keyType.getKind() == TypeKind.TYPEVAR)) {
                    Element elem = element(keyType);

                    imports.add(((QualifiedNameable)(elem)).getQualifiedName().toString());
                    imports.add("java.util.Collection");

                    String typeName = elem.getSimpleName().toString();

                    code.add(indentedLine("for (%s %s : ((Collection<? extends %s>)%s.keySet())) {", typeName, el, typeName, accessor));

                    indent++;
                    code.addAll(keyRes);
                    indent--;

                    code.add(indentedLine("}"));
                }

                if (!valRes.isEmpty() && (valType.getKind() == TypeKind.DECLARED || valType.getKind() == TypeKind.TYPEVAR)) {
                    Element elem = element(valType);

                    imports.add(((QualifiedNameable)(elem)).getQualifiedName().toString());
                    imports.add("java.util.Collection");

                    String typeName = elem.getSimpleName().toString();

                    code.add(indentedLine("for (%s %s : ((Collection<? extends %s>)%s.values())) {", typeName, el, typeName, accessor));

                    indent++;
                    code.addAll(valRes);
                    indent--;

                    code.add(indentedLine("}"));
                }

                indent--;

                code.add(indentedLine("}"));

                if (keyRes.isEmpty() && valRes.isEmpty())
                    return java.util.Collections.emptyList();
                else
                    return code;
            }
            else if (assignableFrom(erasedType(t), type(java.util.Collection.class.getName()))) {
                List<? extends TypeMirror> args = ((DeclaredType)t).getTypeArguments();

                TypeMirror arg = args.get(0);

                if ((arg.getKind() == TypeKind.DECLARED || arg.getKind() == TypeKind.TYPEVAR)) {
                    List<String> code = new ArrayList<>();

                    Element elem = element(arg);

                    imports.add(((QualifiedNameable)(elem)).getQualifiedName().toString());
                    imports.add("java.util.Collection");

                    String el = "e" + indent;

                    code.add(indentedLine("if (%s != null) {", accessor));

                    indent++;

                    String typeName = elem.getSimpleName().toString();

                    code.add(indentedLine("for (%s %s : (Collection<? extends %s>)%s) {", typeName, el, typeName, accessor));

                    indent++;

                    List<String> res = marshall(arg, el, mode);

                    code.addAll(res);

                    indent--;

                    code.add(indentedLine("}"));

                    indent--;

                    code.add(indentedLine("}"));

                    if (res.isEmpty())
                        return java.util.Collections.emptyList();
                    else
                        return code;
                }
            }
        }

        return java.util.Collections.emptyList();
    }

    /** */
    private boolean isCacheObject(TypeMirror type) {
        TypeMirror obj = type("org.apache.ignite.internal.processors.cache.CacheObject");
        return obj != null && assignableFrom(type, obj);
    }

    /** */
    private boolean isMessage(TypeMirror type) {
        TypeMirror msg = type(MESSAGE_INTERFACE);
        return msg != null && assignableFrom(type, msg);
    }

    /** */
    private boolean isNonMarshallableMessage(TypeElement te) {
        TypeMirror nonMarshallable = type("org.apache.ignite.plugin.extensions.communication.NonMarshallableMessage");
        return nonMarshallable != null && assignableFrom(te.asType(), nonMarshallable);
    }

    /** */
    private boolean isCacheMarshallableMessage(TypeElement te) {
        return cacheMarshallableMsgType != null && env.getTypeUtils().isAssignable(te.asType(), cacheMarshallableMsgType);
    }

    /** True if {@code te} extends {@code CacheIdAware}. */
    private boolean isCacheIdAwareMessage(TypeElement te) {
        TypeMirror cacheIdAware = type("org.apache.ignite.plugin.extensions.communication.CacheIdAware");
        return cacheIdAware != null && assignableFrom(te.asType(), cacheIdAware);
    }

    /** True if {@code te} extends {@code GridCacheGroupIdMessage}. */
    private boolean isCacheGroupIdMessage(TypeElement te) {
        TypeMirror grpIdMsg = type("org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage");
        return grpIdMsg != null && assignableFrom(te.asType(), grpIdMsg);
    }

    /** */
    private boolean marshallableMessage() {
        return marshallableMsgType != null && env.getTypeUtils().isAssignable(type.asType(), marshallableMsgType);
    }

    /** */
    /** */
    private Element element(TypeMirror t) {
        return t.getKind() == TypeKind.DECLARED ?
            ((DeclaredType)t).asElement() :
            ((DeclaredType)((TypeVariable)t).getUpperBound()).asElement();
    }
}
