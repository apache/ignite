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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.QualifiedNameable;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import org.apache.ignite.internal.systemview.SystemViewRowAttributeWalkerProcessor;

import static org.apache.ignite.internal.MessageProcessor.CACHE_OBJECT_CLS;
import static org.apache.ignite.internal.MessageProcessor.CUSTOM_WIRE_FORM_MESSAGE_INTERFACE;
import static org.apache.ignite.internal.MessageProcessor.IGNITE_CHECKED_EXCEPTION_CLS;
import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;
import static org.apache.ignite.internal.MessageProcessor.PLAIN_MESSAGE_INTERFACE;

/**
 * Generates the {@code *WireForm} class of a {@code Message}: one walk over its {@code @Order} fields — the loops,
 * null guards and recursion into collections — with the concrete calls supplied by two helpers:
 * {@link MessageWireCalls} for nested messages and cache objects, {@link MarshallingCalls} for what becomes bytes.
 * A message with nothing to walk and nothing to marshal gets no wire form.
 */
public class MessageWireFormGenerator extends MessageCompanionGenerator {
    /** Interface the generated wire forms implement. */
    private static final String MESSAGE_WIRE_FORM_CLS = "org.apache.ignite.plugin.extensions.communication.MessageWireForm";

    /** */
    private static final String MARSHALLER_CLS = "org.apache.ignite.marshaller.Marshaller";

    /** */
    private static final String GRID_KERNAL_CONTEXT_CLS = "org.apache.ignite.internal.GridKernalContext";

    /** */
    private static final String CACHE_OBJECT_CONTEXT_CLS = "org.apache.ignite.internal.processors.cache.CacheObjectContext";

    /** */
    private static final String GRID_CACHE_GROUP_ID_MESSAGE_CLS = "org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage";

    /** */
    private static final String IGNITE_MESSAGE_FACTORY_CLS = "org.apache.ignite.internal.managers.communication.IgniteMessageFactory";

    /** The marshalling calls of the generated class: {@code @Marshalled} fields and the message's own {@code marshal}. */
    private final MarshallingCalls marshalling = new MarshallingCalls(this);

    /** The {@code MessageWire} calls of the generated class: the walk's leaves. */
    private final MessageWireCalls wire = new MessageWireCalls(this);

    /** Accumulated source lines of the generated methods. */
    private final List<String> methods = new ArrayList<>();

    /** */
    private final TypeMirror customWireFormMsgType;

    /** */
    private final TypeMirror msgType;

    /** */
    private final TypeMirror cacheObjType;

    /** */
    private final TypeMirror plainType;

    /** */
    private final TypeMirror mapType;

    /** */
    private final TypeMirror colType;

    /** */
    private final TypeMirror cacheGrpIdMsgType;

    /** Whether the message is a {@code CustomWireFormMessage}, so the generated methods call its own step. */
    private boolean customWireForm;

    /** Whether any generated method got a non-empty body; a wire form without one is skipped entirely. */
    private boolean hasStatements;

    /** Whether the currently generated method emitted a loop-nested {@code MessageWire} call and so needs the {@code msgFactory} local. */
    boolean usesMsgFactory;

    /** Nesting depth of the current for-loop; names loop variables {@code e}, {@code e1}, {@code e2}… */
    int loopDepth;

    /** */
    MessageWireFormGenerator(ProcessingEnvironment env) {
        super(env);

        customWireFormMsgType = type(CUSTOM_WIRE_FORM_MESSAGE_INTERFACE);
        msgType = type(MESSAGE_INTERFACE);
        cacheObjType = type(CACHE_OBJECT_CLS);
        plainType = type(PLAIN_MESSAGE_INTERFACE);
        mapType = type(Map.class.getName());
        colType = type(Collection.class.getName());
        cacheGrpIdMsgType = type(GRID_CACHE_GROUP_ID_MESSAGE_CLS);
    }

    /** {@inheritDoc} */
    @Override protected String typeSuffix() {
        return "WireForm";
    }

    /** {@inheritDoc} */
    @Override protected void generateBody(List<VariableElement> fields) {
        marshalling.readFields(enclosedFields());

        customWireForm = customWireFormMsgType != null && assignableFrom(type.asType(), customWireFormMsgType);

        generatePrepareMethod(fields);
        generateRestoreMethods(fields);
    }

    /** {@inheritDoc} */
    @Override protected String buildClassCode(String clsName) throws IOException {
        if (!hasStatements)
            return null;

        try (Writer writer = new StringWriter()) {
            imports.add(type.toString());
            imports.add(MESSAGE_WIRE_FORM_CLS);

            if (marshalling.needsMarshaller())
                imports.add(MARSHALLER_CLS);

            writeClassHeader(writer, "MessageWireForm", clsName);

            writer.write(" {" + NL);

            writeConstructor(writer, clsName);

            for (String line : methods)
                writer.write(line + NL);

            writer.write("}");

            return writer.toString();
        }
    }

    /** Writes the {@code marshaller} field and the constructor initializing it, when the marshaller is needed. */
    private void writeConstructor(Writer writer, String clsName) throws IOException {
        if (!marshalling.needsMarshaller())
            return;

        writer.write(indentedLine(METHOD_JAVADOC));
        writer.write(NL);
        writer.write(indentedLine("private final Marshaller marshaller;"));
        writer.write(NL + NL);

        writer.write(indentedLine(METHOD_JAVADOC));
        writer.write(NL);
        writer.write(indentedLine("public " + clsName + "(Marshaller marshaller) {"));
        writer.write(NL);

        indent++;

        writer.write(indentedLine("this.marshaller = marshaller;"));
        writer.write(NL);

        indent--;

        writer.write(indentedLine("}"));
        writer.write(NL + NL);
    }

    /** @return {@code true} if the generated {@code code} refers to the cache object context, so it has to be resolved. */
    private static boolean usesCtx(List<String> code) {
        return code.stream().anyMatch(l -> l.matches(".*\\bctx\\b.*"));
    }

    /**
     * Returns whether the {@code @Order} fields of {@code msgType} need a cache object context to unmarshal. Such a
     * message must not be a {@code @NioField}: its {@code unmarshalNio} runs on the NIO thread, which has no context.
     * A type with no fields to inspect (e.g. a type variable) is conservatively assumed to need the context.
     */
    private boolean nestedNeedsCtx(TypeMirror type) {
        Element el = env.getTypeUtils().asElement(type);

        if (!(el instanceof TypeElement))
            return true;

        return SystemViewRowAttributeWalkerProcessor.superclasses(env, (TypeElement)el)
            .flatMap(c -> ElementFilter.fieldsIn(c.getEnclosedElements()).stream())
            .filter(f -> f.getAnnotation(Order.class) != null)
            .anyMatch(f -> needsCtx(f.asType()));
    }

    /** */
    private static boolean isNioField(VariableElement field) {
        return field.getAnnotation(NioField.class) != null;
    }

    /**
     * A field of the message keeps its type arguments, so a for-each over it already yields the element type. Inside
     * a loop it does not: the loop variable is declared by simple name, {@code Map e} rather than {@code Map<K, V> e},
     * and so is raw. A type variable gives nothing to iterate by either.
     *
     * @return {@code true} if the for-each needs the element type spelled out with a cast.
     */
    private boolean needsCast(TypeMirror elemType) {
        return loopDepth > 0 || elemType.getKind() == TypeKind.TYPEVAR;
    }

    /** Prefixes {@code body} with the {@code msgFactory} resolution line when a loop-nested {@code MessageWire} call was emitted. */
    private void prependMsgFactoryResolution(List<String> body) {
        if (!usesMsgFactory)
            return;

        imports.add(IGNITE_MESSAGE_FACTORY_CLS);

        body.add(0, EMPTY);
        body.add(0, indentedLine("IgniteMessageFactory msgFactory = (IgniteMessageFactory)kctx.messageFactory();"));
    }

    /**
     * Returns the {@code CacheObjectContext ctx} resolution line for the current message type. Cache messages resolve
     * via the cache, group messages via the cache group — the group's context outlives the stop of individual caches,
     * so cache objects still read back while a cache (group) is being destroyed.
     */
    private String ctxResolutionLine() {
        if (isCacheIdAwareMessage(type))
            return indentedLine("CacheObjectContext ctx = cacheObjCtx != null ? cacheObjCtx : " +
                    "kctx.cache().context().cacheObjectContext(msg.cacheId());");
        else if (isCacheGroupIdMessage(type))
            return indentedLine("CacheObjectContext ctx = cacheObjCtx != null ? cacheObjCtx : " +
                    "kctx.cache().cacheGroup(msg.groupId()) == null ? null : " +
                    "kctx.cache().cacheGroup(msg.groupId()).cacheObjectContext();");
        else
            return indentedLine("CacheObjectContext ctx = cacheObjCtx;");
    }

    /** Generates the {@code prepare} method: the message's own step, then its bytes, then the walk. */
    private void generatePrepareMethod(List<VariableElement> orderedFields) {
        imports.add(IGNITE_CHECKED_EXCEPTION_CLS);
        imports.add(GRID_KERNAL_CONTEXT_CLS);
        imports.add(CACHE_OBJECT_CONTEXT_CLS);

        String signature = "prepare(" + simpleNameWithGeneric(type) + " msg, GridKernalContext kctx, CacheObjectContext cacheObjCtx)";

        hasStatements |= emitMethod(methods, signature, body -> {
            usesMsgFactory = false;

            List<String> code = new ArrayList<>();

            if (customWireForm)
                appendBlock(code, List.of(indentedLine("msg.toWireForm();")));

            appendBlock(code, marshalling.prepare());
            appendFields(code, orderedFields, Direction.OUT);

            if (code.isEmpty())
                return;

            if (needsCtx(orderedFields) || usesCtx(code))
                appendBlock(body, List.of(ctxResolutionLine()));

            appendBlock(body, code);

            prependMsgFactoryResolution(body);
        });
    }

    /** Generates the {@code restore} overloads: the NIO-eligible fields apart from the rest. */
    private void generateRestoreMethods(List<VariableElement> orderedFields) {
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

        generateRestoreMethod(msgParam + ", CacheObjectContext cacheObjCtx, ClassLoader clsLdr", workerFields);

        if (!nioFields.isEmpty())
            generateRestoreNioMethod(msgParam, nioFields);
    }

    /** Generates the cache-aware {@code restore} overload: the walk, then the bytes, then the message's own step. */
    private void generateRestoreMethod(String params, List<VariableElement> fields) {
        hasStatements |= emitMethod(methods, "restore(" + params + ")", body -> {
            usesMsgFactory = false;

            List<String> code = new ArrayList<>();

            appendFields(code, fields, Direction.IN);
            appendBlock(code, marshalling.restore());

            if (customWireForm)
                appendBlock(code, List.of(indentedLine("msg.fromWireForm();")));

            if (code.isEmpty())
                return;

            if (needsCtx(fields) || usesCtx(code))
                appendBlock(body, List.of(ctxResolutionLine()));

            appendBlock(body, code);

            prependMsgFactoryResolution(body);
        });
    }

    /** Generates the {@code restoreNio} method for NIO-eligible {@code @Message} fields. */
    private void generateRestoreNioMethod(String params, List<VariableElement> nioFields) {
        hasStatements |= emitMethod(methods, "restoreNio(" + params + ")", body -> {
            for (VariableElement f : nioFields)
                appendBlock(body, wire.forNioMessage(fieldAccessor(f)));
        });
    }

    /** Generates the code of each field and appends the non-empty results to {@code body}. */
    private void appendFields(List<String> body, List<VariableElement> fields, Direction dir) {
        for (VariableElement field : fields) {
            List<String> result = codeFor(field.asType(), fieldAccessor(field), dir);

            if (!result.isEmpty())
                appendBlock(body, result);
        }
    }

    /**
     * @return the generated code lines for a field of type {@code t} in the given direction, or empty if it needs none.
     */
    private List<String> codeFor(TypeMirror t, String accessor, Direction dir) {
        if (t.getKind() == TypeKind.ARRAY) {
            TypeMirror comp = ((ArrayType)t).getComponentType();

            return comp.getKind() == TypeKind.DECLARED ? arrayCode(comp, accessor, dir) : List.of();
        }

        if (t.getKind() == TypeKind.DECLARED || t.getKind() == TypeKind.TYPEVAR) {
            if (isMessage(t))
                return isPlain(t) ? List.of() : wire.forMessage(accessor, dir);
            if (isCacheObject(t))
                return wire.forCacheObject(accessor, dir);
            if (isMap(t))
                return mapCode((DeclaredType)t, accessor, dir);
            if (isCollection(t))
                return collectionCode((DeclaredType)t, accessor, dir);
        }

        return List.of();
    }

    /** Generates a null-guarded for-each loop over the array's elements. */
    private List<String> arrayCode(TypeMirror comp, String accessor, Direction dir) {
        Element elem = ((DeclaredType)comp).asElement();

        indent++;

        List<String> loopCode = forLoop(elem.getSimpleName().toString(), comp, accessor, dir);

        indent--;

        if (!loopCode.isEmpty())
            imports.add(((QualifiedNameable)elem).getQualifiedName().toString());

        return wrapNullGuarded(accessor, loopCode);
    }

    /** Generates a null-guarded for-each loop over the collection's elements. */
    private List<String> collectionCode(DeclaredType t, String accessor, Direction dir) {
        TypeMirror arg = t.getTypeArguments().get(0);

        if (arg.getKind() != TypeKind.DECLARED && arg.getKind() != TypeKind.TYPEVAR)
            return List.of();

        Element elem = element(arg);

        String typeName = elem.getSimpleName().toString();

        indent++;

        String iterable = needsCast(arg) ? "(Collection<? extends " + typeName + ">)" + accessor : accessor;

        List<String> loopCode = forLoop(typeName, arg, iterable, dir);

        indent--;

        if (!loopCode.isEmpty()) {
            imports.add(((QualifiedNameable)elem).getQualifiedName().toString());
            imports.add(Collection.class.getName());
        }

        return wrapNullGuarded(accessor, loopCode);
    }

    /** Iterates {@code keySet()} then {@code values()}, wrapping both loops in a null-guard. */
    private List<String> mapCode(DeclaredType t, String accessor, Direction dir) {
        List<? extends TypeMirror> args = t.getTypeArguments();

        indent++;

        List<String> combined = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            TypeMirror elemType = args.get(i);

            if (elemType.getKind() != TypeKind.DECLARED && elemType.getKind() != TypeKind.TYPEVAR)
                continue;

            Element elem = element(elemType);

            String typeName = elem.getSimpleName().toString();
            String collection = i == 0 ? "keySet" : "values";
            String iterable = needsCast(elemType)
                ? "((Collection<? extends " + typeName + ">)" + accessor + "." + collection + "())"
                : accessor + "." + collection + "()";

            List<String> loopCode = forLoop(typeName, elemType, iterable, dir);

            if (loopCode.isEmpty())
                continue;

            imports.add(((QualifiedNameable)elem).getQualifiedName().toString());
            imports.add(Collection.class.getName());

            combined.addAll(loopCode);
        }

        indent--;

        return wrapNullGuarded(accessor, combined);
    }

    /** @return a for-each loop over {@code iterable}, or empty when its elements need no code of their own. */
    private List<String> forLoop(String typeName, TypeMirror elemType, String iterable, Direction dir) {
        String el = loopDepth == 0 ? "e" : "e" + loopDepth;

        loopDepth++;
        indent++;

        List<String> inner = codeFor(elemType, el, dir);

        indent--;
        loopDepth--;

        if (inner.isEmpty())
            return List.of();

        List<String> code = new ArrayList<>();

        code.add(indentedLine("for (%s %s : %s) {", typeName, el, iterable));

        code.addAll(inner);

        code.add(indentedLine("}"));

        return code;
    }

    /** Returns empty if {@code inner} is empty; otherwise wraps {@code inner} in a null-guard on {@code nullGuard}. */
    private List<String> wrapNullGuarded(String nullGuard, List<String> inner) {
        if (inner.isEmpty())
            return List.of();

        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null) {", nullGuard));

        code.addAll(inner);

        code.add(indentedLine("}"));

        return code;
    }

    /** Which way the generated field code runs. */
    enum Direction {
        /** On the way out: the code runs before the message is written. */
        OUT,

        /** On the way in: the code runs after the message is read, with cache context and class loader at hand. */
        IN
    }

    /** Returns {@code true} if any field requires {@code ctx} in generated marshal/unmarshal code. */
    private boolean needsCtx(List<VariableElement> fields) {
        return fields.stream().anyMatch(f -> needsCtx(f.asType()));
    }

    /** Returns {@code true} if type {@code t} (or its element/key/value types) requires {@code ctx}. */
    private boolean needsCtx(TypeMirror t) {
        if (t.getKind() == TypeKind.ARRAY)
            return needsCtx(((ArrayType)t).getComponentType());

        if (t.getKind() == TypeKind.DECLARED || t.getKind() == TypeKind.TYPEVAR) {
            if (isMessage(t))
                return !isPlain(t);

            if (isCacheObject(t))
                return true;

            if (isMap(t)) {
                List<? extends TypeMirror> args = ((DeclaredType)t).getTypeArguments();
                return needsCtx(args.get(0)) || needsCtx(args.get(1));
            }

            if (isCollection(t)) {
                List<? extends TypeMirror> args = ((DeclaredType)t).getTypeArguments();
                return needsCtx(args.get(0));
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean shouldSkip(TypeElement type, List<VariableElement> fields) {
        return isPlain(type.asType());
    }

    /** Recursion skip for such fields is subtype-safe: subclasses inherit the {@code PlainMessage} marker. */
    private boolean isPlain(TypeMirror t) {
        return assignableFrom(t, plainType);
    }

    /** */
    private boolean isMessage(TypeMirror type) {
        return assignableFrom(type, msgType);
    }

    /** */
    private boolean isCacheObject(TypeMirror type) {
        return assignableFrom(type, cacheObjType);
    }

    /** Returns {@code true} if {@code type} (erased) is assignable to {@code java.util.Map}. */
    private boolean isMap(TypeMirror type) {
        return assignableFrom(erasedType(type), mapType);
    }

    /** Returns {@code true} if {@code type} (erased) is assignable to {@code java.util.Collection}. */
    private boolean isCollection(TypeMirror type) {
        return assignableFrom(erasedType(type), colType);
    }

    /** */
    private boolean isCacheGroupIdMessage(TypeElement te) {
        return assignableFrom(te.asType(), cacheGrpIdMsgType);
    }
}
