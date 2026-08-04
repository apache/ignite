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
import java.util.Set;
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
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import org.apache.ignite.internal.systemview.SystemViewRowAttributeWalkerProcessor;

import static org.apache.ignite.internal.MessageProcessor.CACHE_OBJECT_CLS;
import static org.apache.ignite.internal.MessageProcessor.IGNITE_CHECKED_EXCEPTION_CLS;
import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;
import static org.apache.ignite.internal.MessageProcessor.PLAIN_MESSAGE_INTERFACE;

/**
 * Generates the {@code *WireForm} class of a {@code Message}: the code that walks into its nested messages and
 * prepares its cache objects, so the fields are in the shape they go on the wire in. Marshalling a field is a step of
 * its own and belongs to {@link MessageMarshallerGenerator}; a message with nothing to walk gets no wire form.
 */
public class MessageWireFormGenerator extends MessageWireCompanionGenerator {
    /** Interface the generated wire forms implement. */
    private static final String MESSAGE_WIRE_FORM_CLS = "org.apache.ignite.plugin.extensions.communication.MessageWireForm";

    /** Facade the generated code calls to take nested messages to the wire and back. */
    private static final String MESSAGE_WIRE_CLS = "org.apache.ignite.internal.managers.communication.MessageWire";

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

    /** Nesting depth of the current for-loop; names loop variables {@code e}, {@code e1}, {@code e2}… */
    private int loopDepth;

    /** */
    MessageWireFormGenerator(ProcessingEnvironment env) {
        super(env);

        msgType = type(MESSAGE_INTERFACE);
        cacheObjType = type(CACHE_OBJECT_CLS);
        plainType = type(PLAIN_MESSAGE_INTERFACE);
        mapType = type(Map.class.getName());
        colType = type(Collection.class.getName());
    }

    /** {@inheritDoc} */
    @Override protected String typeSuffix() {
        return "WireForm";
    }

    /** {@inheritDoc} */
    @Override protected void generateBody(List<VariableElement> fields) {
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

    /** Cache-free read-back of a {@code @NioField} message field on the NIO thread (no cache context available). */
    private List<String> fromWireNioField(String accessor) {
        imports.add(MESSAGE_WIRE_CLS);

        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null)", accessor));

        indent++;

        code.add(indentedLine("MessageWire.fromWire(%s, kctx);", accessor));

        indent--;

        return code;
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
            .anyMatch(f -> needsCtxType(f.asType()));
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

            appendBlock(body, code);

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

            List<String> code = new ArrayList<>();

            appendFields(code, fields, Direction.IN);

            if (code.isEmpty())
                return;

            if (needsCtx(fields))
                appendBlock(body, List.of(ctxResolutionLine()));

            appendBlock(body, code);

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

    /** Generates the code of each field and appends the non-empty results to {@code body}. */
    private void appendFields(List<String> body, List<VariableElement> fields, Direction mode) {
        appendFields(body, fields, mode, Set.of());
    }

    /** Generates the code of each field, skipping names in {@code skip}, and appends the non-empty results. */
    private void appendFields(List<String> body, List<VariableElement> fields, Direction mode, Set<String> skip) {
        for (VariableElement field : fields) {
            if (skip.contains(field.getSimpleName().toString()))
                continue;

            List<String> result = codeFor(field.asType(), fieldAccessor(field), mode);

            if (!result.isEmpty())
                appendBlock(body, result);
        }
    }

    /**
     * @return the generated code lines for a field of type {@code t} in the given direction, or empty if it needs none.
     */
    private List<String> codeFor(TypeMirror t, String accessor, Direction mode) {
        if (t.getKind() == TypeKind.ARRAY) {
            TypeMirror comp = ((ArrayType)t).getComponentType();

            return comp.getKind() == TypeKind.DECLARED ? arrayCode(comp, accessor, mode) : List.of();
        }

        if (t.getKind() == TypeKind.DECLARED || t.getKind() == TypeKind.TYPEVAR) {
            if (isMessage(t))
                return isPlain(t) ? List.of() : messageCode(accessor, mode);
            if (isCacheObject(t))
                return cacheObjectCode(accessor, mode);
            if (isMap(t))
                return mapCode((DeclaredType)t, accessor, mode);
            if (isCollection(t))
                return collectionCode((DeclaredType)t, accessor, mode);
        }

        return List.of();
    }

    /**
     * Generates a null-guarded {@code MessageWire} call. Loop-nested calls go through the overloads taking the
     * pre-resolved {@code msgFactory} local (see {@link #prependMsgFactoryResolution}), so the factory is not
     * re-resolved from the context on every element.
     */
    private List<String> messageCode(String accessor, Direction mode) {
        imports.add(MESSAGE_WIRE_CLS);

        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null)", accessor));

        indent++;

        if (loopDepth > 0) {
            usesMsgFactory = true;

            code.add(mode == Direction.OUT
                ? indentedLine("MessageWire.toWire(msgFactory, %s, kctx, ctx);", accessor)
                : indentedLine("MessageWire.fromWire(msgFactory, %s, kctx, ctx, clsLdr);", accessor));
        }
        else {
            code.add(mode == Direction.OUT
                ? indentedLine("MessageWire.toWire(%s, kctx, ctx);", accessor)
                : indentedLine("MessageWire.fromWire(%s, kctx, ctx, clsLdr);", accessor));
        }

        indent--;

        return code;
    }

    /** Generates a null-and-ctx-guarded call that prepares a {@code CacheObject} field, or reads it back. */
    private List<String> cacheObjectCode(String accessor, Direction mode) {
        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null && ctx != null)", accessor));

        indent++;

        code.add(mode == Direction.OUT
            ? indentedLine("%s.marshal(ctx);", accessor)
            : indentedLine("%s.unmarshal(ctx, clsLdr);", accessor));

        indent--;

        return code;
    }

    /** Generates a null-guarded for-each loop over the array's elements. */
    private List<String> arrayCode(TypeMirror comp, String accessor, Direction mode) {
        Element elem = ((DeclaredType)comp).asElement();

        indent++;

        List<String> loopCode = forLoop(elem.getSimpleName().toString(), comp, accessor, mode);

        indent--;

        if (!loopCode.isEmpty())
            imports.add(((QualifiedNameable)elem).getQualifiedName().toString());

        return wrapNullGuarded(accessor, loopCode);
    }

    /** Generates a null-guarded for-each loop over the collection's elements. */
    private List<String> collectionCode(DeclaredType t, String accessor, Direction mode) {
        TypeMirror arg = t.getTypeArguments().get(0);

        if (arg.getKind() != TypeKind.DECLARED && arg.getKind() != TypeKind.TYPEVAR)
            return List.of();

        Element elem = element(arg);

        String typeName = elem.getSimpleName().toString();

        indent++;

        String iterable = needsCast(arg) ? "(Collection<? extends " + typeName + ">)" + accessor : accessor;

        List<String> loopCode = forLoop(typeName, arg, iterable, mode);

        indent--;

        if (!loopCode.isEmpty()) {
            imports.add(((QualifiedNameable)elem).getQualifiedName().toString());
            imports.add(Collection.class.getName());
        }

        return wrapNullGuarded(accessor, loopCode);
    }

    /** Iterates {@code keySet()} then {@code values()}, wrapping both loops in a null-guard. */
    private List<String> mapCode(DeclaredType t, String accessor, Direction mode) {
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

            List<String> loopCode = forLoop(typeName, elemType, iterable, mode);

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
    private List<String> forLoop(String typeName, TypeMirror elemType, String iterable, Direction mode) {
        String el = loopDepth == 0 ? "e" : "e" + loopDepth;

        loopDepth++;
        indent++;

        List<String> inner = codeFor(elemType, el, mode);

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

    /** Which way the generated field code runs: {@link #OUT} before the message is written, {@link #IN} after it is read. */
    private enum Direction {
        /** On the way out: the code runs before the message is written. */
        OUT,

        /** On the way in: the code runs after the message is read, with cache context and class loader at hand. */
        IN
    }

    /** Returns the element for {@code t}; for a type variable, uses its upper bound. */
    private Element element(TypeMirror t) {
        return t.getKind() == TypeKind.DECLARED
            ? ((DeclaredType)t).asElement()
            : ((DeclaredType)((TypeVariable)t).getUpperBound()).asElement();
    }

    /** Returns {@code true} if any field requires {@code ctx} in generated marshal/unmarshal code. */
    private boolean needsCtx(List<VariableElement> fields) {
        return fields.stream().anyMatch(f -> needsCtxType(f.asType()));
    }

    /** Returns {@code true} if type {@code t} (or its element/key/value types) requires {@code ctx}. */
    private boolean needsCtxType(TypeMirror t) {
        if (t.getKind() == TypeKind.ARRAY)
            return needsCtxType(((ArrayType)t).getComponentType());

        if (t.getKind() == TypeKind.DECLARED || t.getKind() == TypeKind.TYPEVAR) {
            if (isMessage(t))
                return !isPlain(t);

            if (isCacheObject(t))
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
}
