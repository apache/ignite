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
import javax.tools.Diagnostic;

import static org.apache.ignite.internal.MessageProcessor.MARSHALLABLE_MESSAGE_INTERFACE;
import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;

/**
 * Generates marshaller class for a given {@code Message} class. The generated marshaller follows the naming convention:
 * {@code org.apache.ignite.internal.codegen.[MessageClassName]Marshaller}.
 * <p>
 * No marshaller is generated for {@code NonMarshallableMessage} types.
 */
public class MessageMarshallerGenerator extends MessageGenerator {
    /** Accumulated source lines for all generated marshal/unmarshal methods. */
    private final List<String> marshall = new ArrayList<>();

    /** MarshallableMessage type mirror. */
    private final TypeMirror marshallableMsgType;

    /** Message type mirror. */
    private final TypeMirror messageMirror;

    /** CacheObject type mirror. */
    private final TypeMirror cacheObjectMirror;

    /** NonMarshallableMessage type mirror. */
    private final TypeMirror nonMarshallableMirror;

    /** CacheIdAware type mirror. */
    private final TypeMirror cacheIdAwareMirror;

    /** GridCacheGroupIdMessage type mirror. */
    private final TypeMirror cacheGroupIdMsgMirror;

    /** java.util.Map type mirror. */
    private final TypeMirror mapMirror;

    /** java.util.Collection type mirror. */
    private final TypeMirror collectionMirror;

    /** CacheMarshallableMessage type mirror. */
    private final TypeMirror cacheMarshallableMsgType;

    /** Whether the current message type implements {@code MarshallableMessage}. */
    private boolean marshallable;

    /** Whether the current message type implements {@code CacheMarshallableMessage}. */
    private boolean cacheMarshallable;

    /** */
    MessageMarshallerGenerator(ProcessingEnvironment env) {
        super(env);

        marshallableMsgType = type(MARSHALLABLE_MESSAGE_INTERFACE);
        cacheMarshallableMsgType = type("org.apache.ignite.plugin.extensions.communication.CacheMarshallableMessage");
        messageMirror = type(MESSAGE_INTERFACE);
        cacheObjectMirror = type("org.apache.ignite.internal.processors.cache.CacheObject");
        nonMarshallableMirror = type("org.apache.ignite.plugin.extensions.communication.NonMarshallableMessage");
        cacheIdAwareMirror = type("org.apache.ignite.plugin.extensions.communication.CacheIdAware");
        cacheGroupIdMsgMirror = type("org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage");
        mapMirror = type("java.util.Map");
        collectionMirror = type("java.util.Collection");
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
        marshallable = marshallableMsgType != null && assignableFrom(type.asType(), marshallableMsgType);
        cacheMarshallable = cacheMarshallableMsgType != null && assignableFrom(type.asType(), cacheMarshallableMsgType);
        
        indent = 1;

        generatePrepareMarshalMethod(fields);
        generateUnmarshallMethods(fields);
    }

    /** {@inheritDoc} */
    @Override String buildClassCode(String marshallerClsName) throws IOException {
        try (Writer writer = new StringWriter()) {
            imports.add(type.toString());
            imports.add("org.apache.ignite.plugin.extensions.communication.MessageMarshaller");

            if (marshallable)
                imports.add("org.apache.ignite.marshaller.Marshaller");

            writeClassHeader(writer, "MessageMarshaller", marshallerClsName);
            
            writer.write(" {" + NL);

            writeConstructor(writer, marshallerClsName);

            for (String line : marshall)
                writer.write(line + NL);

            writer.write("}");

            return writer.toString();
        }
    }

    /** */
    private void writeConstructor(Writer writer, String marshallerClsName) throws IOException {
        writer.write(indentedLine(METHOD_JAVADOC));
        writer.write(NL);

        if (marshallable) {
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
    private void generatePrepareMarshalMethod(List<VariableElement> orderedFields) {
        imports.add("org.apache.ignite.IgniteCheckedException");
        imports.add("org.apache.ignite.internal.GridKernalContext");
        imports.add("org.apache.ignite.internal.processors.cache.GridCacheContext");

        marshall.add(indentedLine(METHOD_JAVADOC));

        marshall.add(indentedLine(
            "@Override public void prepareMarshal(" + simpleNameWithGeneric(type) +
                " msg, GridKernalContext kctx, GridCacheContext<?, ?> nested) throws IgniteCheckedException {"));

        indent++;

        List<String> body = new ArrayList<>();

        if (needsCtx(orderedFields))
            appendBlock(body, List.of(ctxResolutionLine()));

        if (marshallable)
            appendBlock(body, List.of(indentedLine("msg.prepareMarshal(marshaller);")));

        appendFields(body, orderedFields, MarshalMode.PREPARE);

        marshall.addAll(body);

        indent--;

        marshall.add(indentedLine("}"));
    }

    /** */
    private void generateUnmarshallMethods(List<VariableElement> orderedFields) {
        List<VariableElement> nioFields = new ArrayList<>();
        List<VariableElement> workerFields = new ArrayList<>();

        for (VariableElement f : orderedFields) {
            boolean nioField = isNioField(f);

            if (nioField && isMessage(f.asType()))
                nioFields.add(f);
            else {
                if (nioField)
                    env.getMessager().printMessage(Diagnostic.Kind.ERROR,
                        "@NioField has no effect on non-Message field '" + f.getSimpleName() + "' of type " + f.asType(),
                        f);

                workerFields.add(f);
            }
        }

        imports.add("org.apache.ignite.internal.util.typedef.internal.U");

        String msgParam = simpleNameWithGeneric(type) + " msg, GridKernalContext kctx";

        generateFinishUnmarshalMethod("finishUnmarshal", msgParam + ", GridCacheContext<?, ?> nested, ClassLoader clsLdr",
            workerFields, MarshalMode.FINISH_CACHE);

        generateFinishUnmarshalMethod("finishUnmarshal", msgParam, workerFields, MarshalMode.FINISH);

        if (!nioFields.isEmpty())
            generateFinishUnmarshalNioMethod(msgParam, nioFields);
    }

    /** */
    private void generateFinishUnmarshalMethod(String methodName, String params, List<VariableElement> fields, MarshalMode mode) {
        marshall.add(EMPTY);

        marshall.add(indentedLine(METHOD_JAVADOC));

        marshall.add(indentedLine(
            "@Override public void " + methodName + "(" + params + ") throws IgniteCheckedException {"));

        indent++;

        List<String> body = new ArrayList<>();

        if (mode == MarshalMode.FINISH_CACHE && needsCtx(fields))
            appendBlock(body, List.of(ctxResolutionLine()));

        appendFields(body, fields, mode);

        if (marshallable) {
            if (mode == MarshalMode.FINISH_CACHE && cacheMarshallable)
                appendBlock(body, List.of(indentedLine("msg.finishUnmarshal(marshaller, clsLdr);")));
            else if (mode == MarshalMode.FINISH && !cacheMarshallable)
                appendBlock(body, List.of(indentedLine("msg.finishUnmarshal(marshaller, U.resolveClassLoader(kctx.config()));")));
        }

        marshall.addAll(body);

        indent--;

        marshall.add(indentedLine("}"));
    }

    /** */
    private void generateFinishUnmarshalNioMethod(String params, List<VariableElement> nioFields) {
        marshall.add(EMPTY);

        marshall.add(indentedLine(METHOD_JAVADOC));

        marshall.add(indentedLine(
            "@Override public void finishUnmarshalNio(" + params + ") throws IgniteCheckedException {"));

        indent++;

        List<String> body = new ArrayList<>();

        appendFields(body, nioFields, MarshalMode.FINISH);

        marshall.addAll(body);

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

            if (isMap(t)) {
                List<? extends TypeMirror> args = ((DeclaredType)t).getTypeArguments();
                return needsCtxType(args.get(0)) || needsCtxType(args.get(1));
            }

            if (isCollection(t)) {
                List<? extends TypeMirror> args = ((DeclaredType)t).getTypeArguments();
                return needsCtxType(args.get(0));
            }
        }

        return false;
    }

    /** Returns generated marshal/unmarshal code lines for field of type {@code t}, or empty if none needed. */
    private List<String> codeFor(TypeMirror t, String accessor, MarshalMode mode) {
        if (t.getKind() == TypeKind.ARRAY) {
            TypeMirror comp = ((ArrayType)t).getComponentType();

            return comp.getKind() == TypeKind.DECLARED ? marshallArray(comp, accessor, mode) : List.of();
        }

        if (t.getKind() == TypeKind.DECLARED || t.getKind() == TypeKind.TYPEVAR) {
            if (isMessage(t))
                return marshallMessage(accessor, mode);
            if (isCacheObject(t))
                return marshallCacheObject(accessor, mode);
            if (isMap(t))
                return marshallMap((DeclaredType)t, accessor, mode);
            if (isCollection(t))
                return marshallCollection((DeclaredType)t, accessor, mode);
        }

        return List.of();
    }

    /** */
    private List<String> marshallMessage(String accessor, MarshalMode mode) {
        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null)", accessor));

        indent++;

        switch (mode) {
            case PREPARE:
                code.add(indentedLine(
                    "MessageMarshaller.prepareMarshal(kctx.messageFactory(), %s, kctx, ctx);", accessor));
                break;
            case FINISH:
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

    /** */
    private List<String> marshallCacheObject(String accessor, MarshalMode mode) {
        if (mode == MarshalMode.FINISH)
            return List.of();

        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null && ctx != null)", accessor));

        indent++;

        code.add(mode == MarshalMode.PREPARE
            ? indentedLine("%s.prepareMarshal(ctx.cacheObjectContext());", accessor)
            : indentedLine("%s.finishUnmarshal(ctx.cacheObjectContext(), clsLdr);", accessor));

        indent--;

        return code;
    }

    /** */
    private List<String> marshallArray(TypeMirror comp, String accessor, MarshalMode mode) {
        Element elem = ((DeclaredType)comp).asElement();

        imports.add(((QualifiedNameable)elem).getQualifiedName().toString());

        indent++;

        List<String> loopCode = forLoop(elem.getSimpleName().toString(), comp, accessor, mode);

        indent--;

        return wrapNullGuarded(accessor, loopCode);
    }

    /** */
    private List<String> marshallCollection(DeclaredType t, String accessor, MarshalMode mode) {
        TypeMirror arg = t.getTypeArguments().get(0);

        if (arg.getKind() != TypeKind.DECLARED && arg.getKind() != TypeKind.TYPEVAR)
            return List.of();

        Element elem = element(arg);

        imports.add(((QualifiedNameable)elem).getQualifiedName().toString());
        imports.add("java.util.Collection");

        String typeName = elem.getSimpleName().toString();

        indent++;

        List<String> loopCode = forLoop(typeName, arg, "(Collection<? extends " + typeName + ">)" + accessor, mode);

        indent--;

        return wrapNullGuarded(accessor, loopCode);
    }

    /** Iterates {@code keySet()} then {@code values()}, wrapping both loops in a null-guard. */
    private List<String> marshallMap(DeclaredType t, String accessor, MarshalMode mode) {
        List<? extends TypeMirror> args = t.getTypeArguments();

        indent++;

        List<String> combined = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            TypeMirror elemType = args.get(i);

            if (elemType.getKind() != TypeKind.DECLARED && elemType.getKind() != TypeKind.TYPEVAR)
                continue;

            Element elem = element(elemType);

            imports.add(((QualifiedNameable)elem).getQualifiedName().toString());
            imports.add("java.util.Collection");

            String typeName = elem.getSimpleName().toString();
            String collection = i == 0 ? "keySet" : "values";
            String iterable = "((Collection<? extends " + typeName + ">)" + accessor + "." + collection + "())";

            combined.addAll(forLoop(typeName, elemType, iterable, mode));
        }

        indent--;

        return wrapNullGuarded(accessor, combined);
    }

    /** Returns empty if {@code inner} is empty. */
    private List<String> wrapNullGuarded(String nullGuard, List<String> inner) {
        if (inner.isEmpty())
            return List.of();

        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null) {", nullGuard));
        
        code.addAll(inner);
        
        code.add(indentedLine("}"));

        return code;
    }

    /** Returns empty if {@code elemType} requires no marshalling. */
    private List<String> forLoop(String typeName, TypeMirror elemType, String iterable, MarshalMode mode) {
        String el = "e" + (indent + 1);

        indent++;
        
        List<String> inner = codeFor(elemType, el, mode);
        
        indent--;

        if (inner.isEmpty())
            return List.of();

        List<String> code = new ArrayList<>();

        code.add(indentedLine("for (%s %s : %s) {", typeName, el, iterable));
        
        code.addAll(inner);
        
        code.add(indentedLine("}"));

        return code;
    }

    /** */
    private boolean isCacheObject(TypeMirror type) {
        return cacheObjectMirror != null && assignableFrom(type, cacheObjectMirror);
    }

    /** */
    private boolean isMap(TypeMirror type) {
        return mapMirror != null && assignableFrom(erasedType(type), mapMirror);
    }

    /** */
    private boolean isCollection(TypeMirror type) {
        return collectionMirror != null && assignableFrom(erasedType(type), collectionMirror);
    }

    /** */
    private boolean isMessage(TypeMirror type) {
        return messageMirror != null && assignableFrom(type, messageMirror);
    }

    /** */
    private boolean isNonMarshallableMessage(TypeElement te) {
        return nonMarshallableMirror != null && assignableFrom(te.asType(), nonMarshallableMirror);
    }

    /** */
    private boolean isCacheIdAwareMessage(TypeElement te) {
        return cacheIdAwareMirror != null && assignableFrom(te.asType(), cacheIdAwareMirror);
    }

    /** */
    private boolean isCacheGroupIdMessage(TypeElement te) {
        return cacheGroupIdMsgMirror != null && assignableFrom(te.asType(), cacheGroupIdMsgMirror);
    }

    /** Returns the element for {@code t}; for a type variable, uses its upper bound. */
    private Element element(TypeMirror t) {
        return t.getKind() == TypeKind.DECLARED ?
            ((DeclaredType)t).asElement() :
            ((DeclaredType)((TypeVariable)t).getUpperBound()).asElement();
    }

    /** */
    private static boolean isNioField(VariableElement field) {
        return field.getAnnotation(NioField.class) != null;
    }

    /** Marshals each field and appends non-empty results to {@code body}. */
    private void appendFields(List<String> body, List<VariableElement> fields, MarshalMode mode) {
        for (VariableElement field : fields) {
            List<String> result = codeFor(field.asType(), fieldAccessor(field), mode);

            if (!result.isEmpty())
                appendBlock(body, result);
        }
    }

    /** Appends {@code block} to {@code body}, inserting a blank-line separator when {@code body} is non-empty. */
    private static void appendBlock(List<String> body, List<String> block) {
        if (!body.isEmpty())
            body.add(EMPTY);

        body.addAll(block);
    }

    /** */
    private enum MarshalMode {
        /** Marshal. */
        PREPARE,

        /** Lightweight unmarshal. Messages only, CacheObject fields are skipped (no cache context available). */
        FINISH,

        /** Unmarshal with full cache context and class loader. */
        FINISH_CACHE
    }
}
