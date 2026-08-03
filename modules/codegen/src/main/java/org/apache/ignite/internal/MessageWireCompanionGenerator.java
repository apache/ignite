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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
import javax.tools.Diagnostic;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MessageProcessor.CACHE_OBJECT_CLS;
import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;
import static org.apache.ignite.internal.MessageProcessor.NON_MARSHALLABLE_MESSAGE_INTERFACE;

/**
 * Shared ground for the companions that take a {@code Message} to the wire and back: walking its fields, telling a
 * nested message from a cache object from a collection, and the loop and null-guard shapes the generated code uses.
 * What each companion does with a field is left to the subclass.
 */
public abstract class MessageWireCompanionGenerator extends MessageCompanionGenerator {
    /** */
    protected static final String GRID_KERNAL_CONTEXT_CLS = "org.apache.ignite.internal.GridKernalContext";

    /** */
    protected static final String CACHE_OBJECT_CONTEXT_CLS = "org.apache.ignite.internal.processors.cache.CacheObjectContext";

    /** */
    protected static final String GRID_CACHE_GROUP_ID_MESSAGE_CLS = "org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage";

    /** Facade the generated code calls to take nested messages to the wire and back. */
    protected static final String MESSAGE_WIRE_CLS = "org.apache.ignite.internal.managers.communication.MessageWire";

    /** */
    protected static final String IGNITE_MESSAGE_FACTORY_CLS = "org.apache.ignite.internal.managers.communication.IgniteMessageFactory";

    /** {@code IgniteUtils} shortcut used by the generated {@code @Marshalled} handling. */
    protected static final String U_CLS = "org.apache.ignite.internal.util.typedef.internal.U";

    /** Accumulated source lines of the generated methods. */
    protected final List<String> methods = new ArrayList<>();

    /** */
    protected final TypeMirror msgType;

    /** */
    protected final TypeMirror cacheObjType;

    /** */
    protected final TypeMirror nonMarshallableType;

    /** */
    protected final TypeMirror cacheGrpIdMsgType;

    /** */
    protected final TypeMirror mapType;

    /** */
    protected final TypeMirror colType;

    /** Whether any generated method got a non-empty body; a companion without one is skipped entirely. */
    protected boolean hasStatements;

    /** Enclosed fields of the currently processed type. Computed once per {@link #generateBody} call. */
    protected Map<String, VariableElement> enclosed;

    /** {@link MarshalledKind} of each {@code @Marshalled} enclosed field. Computed once per {@link #generateBody} call. */
    protected final Map<VariableElement, MarshalledKind> kinds = new HashMap<>();

    /** Nesting depth of the current for-loop; names loop variables {@code e}, {@code e1}, {@code e2}… */
    protected int loopDepth;

    /** Whether the currently generated method emitted a loop-nested facade call and so needs the {@code msgFactory} local. */
    protected boolean usesMsgFactory;

    /** */
    MessageWireCompanionGenerator(ProcessingEnvironment env) {
        super(env);

        msgType = type(MESSAGE_INTERFACE);
        cacheObjType = type(CACHE_OBJECT_CLS);
        nonMarshallableType = type(NON_MARSHALLABLE_MESSAGE_INTERFACE);
        cacheGrpIdMsgType = type(GRID_CACHE_GROUP_ID_MESSAGE_CLS);
        mapType = type(Map.class.getName());
        colType = type(Collection.class.getName());
    }

    /** Reads the enclosed fields of the current type and the kind of each {@code @Marshalled} one among them. */
    protected void readFields() {
        enclosed = enclosedFields();

        for (VariableElement f : enclosed.values()) {
            MarshalledKind kind = marshalledKind(f);

            if (kind != null)
                kinds.put(f, kind);
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean shouldSkip(TypeElement type, List<VariableElement> fields) {
        return isNonMarshallable(type.asType());
    }

    /** Generates the code of each field and appends the non-empty results to {@code body}. */
    protected void appendFields(List<String> body, List<VariableElement> fields, Direction mode) {
        appendFields(body, fields, mode, Set.of());
    }

    /** Generates the code of each field, skipping names in {@code skip}, and appends the non-empty results. */
    protected void appendFields(List<String> body, List<VariableElement> fields, Direction mode, Set<String> skip) {
        for (VariableElement field : fields) {
            if (skip.contains(field.getSimpleName().toString()))
                continue;

            List<String> result = codeFor(field.asType(), fieldAccessor(field), mode);

            if (!result.isEmpty())
                appendBlock(body, result);
        }
    }

    /** @return the generated code lines for a field of type {@code t} in the given direction, or empty if it needs none. */
    protected List<String> codeFor(TypeMirror t, String accessor, Direction mode) {
        if (t.getKind() == TypeKind.ARRAY) {
            TypeMirror comp = ((ArrayType)t).getComponentType();

            return comp.getKind() == TypeKind.DECLARED ? arrayCode(comp, accessor, mode) : List.of();
        }

        if (t.getKind() == TypeKind.DECLARED || t.getKind() == TypeKind.TYPEVAR) {
            if (isMessage(t))
                return isNonMarshallable(t) ? List.of() : messageCode(accessor, mode);
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

        List<String> loopCode = forLoop(typeName, arg, "(Collection<? extends " + typeName + ">)" + accessor, mode);

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
            String iterable = "((Collection<? extends " + typeName + ">)" + accessor + "." + collection + "())";

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

    /** Prefixes {@code body} with the {@code msgFactory} resolution line when a loop-nested facade call was emitted. */
    protected void prependMsgFactoryResolution(List<String> body) {
        if (!usesMsgFactory)
            return;

        imports.add(IGNITE_MESSAGE_FACTORY_CLS);

        body.add(0, EMPTY);
        body.add(0, indentedLine("IgniteMessageFactory msgFactory = (IgniteMessageFactory)kctx.messageFactory();"));
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

    /**
     * Returns the {@code CacheObjectContext ctx} resolution line for the current message type. Cache messages resolve
     * via the cache, group messages via the cache group — the group's context outlives the stop of individual caches,
     * so cache objects still unmarshal while a cache (group) is being destroyed.
     */
    protected String ctxResolutionLine() {
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

    /** Returns {@code true} if any field requires {@code ctx} in generated marshal/unmarshal code. */
    protected boolean needsCtx(List<VariableElement> fields) {
        return fields.stream().anyMatch(f -> needsCtxType(f.asType()));
    }

    /** Returns {@code true} if type {@code t} (or its element/key/value types) requires {@code ctx}. */
    protected boolean needsCtxType(TypeMirror t) {
        if (t.getKind() == TypeKind.ARRAY)
            return needsCtxType(((ArrayType)t).getComponentType());

        if (t.getKind() == TypeKind.DECLARED || t.getKind() == TypeKind.TYPEVAR) {
            if (isMessage(t))
                return !isNonMarshallable(t);

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

    /** */
    protected boolean isMessage(TypeMirror type) {
        return assignableFrom(type, msgType);
    }

    /** */
    protected boolean isCacheObject(TypeMirror type) {
        return assignableFrom(type, cacheObjType);
    }

    /** Returns {@code true} if {@code type} (erased) is assignable to {@code java.util.Map}. */
    protected boolean isMap(TypeMirror type) {
        return assignableFrom(erasedType(type), mapType);
    }

    /** Returns {@code true} if {@code type} (erased) is assignable to {@code java.util.Collection}. */
    protected boolean isCollection(TypeMirror type) {
        return assignableFrom(erasedType(type), colType);
    }

    /** Recursion skip for such fields is subtype-safe: subclasses inherit the {@code NonMarshallableMessage} marker. */
    protected boolean isNonMarshallable(TypeMirror t) {
        return assignableFrom(t, nonMarshallableType);
    }

    /** */
    protected boolean isCacheGroupIdMessage(TypeElement te) {
        return assignableFrom(te.asType(), cacheGrpIdMsgType);
    }

    /** Returns the element for {@code t}; for a type variable, uses its upper bound. */
    protected Element element(TypeMirror t) {
        return t.getKind() == TypeKind.DECLARED
            ? ((DeclaredType)t).asElement()
            : ((DeclaredType)((TypeVariable)t).getUpperBound()).asElement();
    }

    /** Marshalling flavour of a {@code @Marshalled} field, told apart by the shape of its companion wire field(s). */
    protected enum MarshalledKind {
        /** {@code byte[]} companion: the whole object is a single marshaller blob. */
        BLOB,

        /** {@code Message[]} companion: per-element {@code Message} serialization, the collection is rebuilt on unmarshal. */
        ELEMENTS,

        /** {@code Collection<byte[]>} companion: per-element marshaller blobs, each element keeping its own class loader. */
        ELEMENT_BLOBS,

        /** Two companions ({@code keys()}/{@code values()}): a {@code Map} serialized as parallel wire fields. */
        MAP
    }

    /**
     * @return the flavour of {@code field}'s {@code @Marshalled}, or {@code null} when the field is not annotated.
     * Called once per field when building {@link #kinds}; look the kind up there instead.
     */
    protected @Nullable MarshalledKind marshalledKind(VariableElement field) {
        Marshalled ann = field.getAnnotation(Marshalled.class);

        if (ann == null)
            return null;

        boolean map = !ann.keys().isEmpty() || !ann.values().isEmpty();

        if (map == !ann.value().isEmpty() || (map && (ann.keys().isEmpty() || ann.values().isEmpty()))) {
            env.getMessager().printMessage(Diagnostic.Kind.ERROR,
                "@Marshalled must set either value() or both keys() and values()", field);

            return null;
        }

        if (map)
            return MarshalledKind.MAP;

        TypeMirror wire = requireEnclosed(enclosed, ann.value(), "@Marshalled").asType();

        if (wire.getKind() == TypeKind.ARRAY) {
            return ((ArrayType)wire).getComponentType().getKind() == TypeKind.BYTE
                ? MarshalledKind.BLOB
                : MarshalledKind.ELEMENTS;
        }

        return MarshalledKind.ELEMENT_BLOBS;
    }

    /** Returns the enclosed field named {@code name}, or throws if absent. */
    protected VariableElement requireEnclosed(Map<String, VariableElement> enclosed, String name, String annotationName) {
        VariableElement el = enclosed.get(name);

        if (el == null)
            throw new IllegalStateException(annotationName + " companion field '" + name + "' not found in " + type);

        return el;
    }

    /** Returns names of wire fields skipped by {@link #appendFields} in UNMARSHAL mode. */
    protected Set<String> marshalledWireFieldsToSkip() {
        Set<String> names = new HashSet<>();

        for (VariableElement f : enclosed.values()) {
            MarshalledKind kind = kinds.get(f);

            if (kind == null || kind == MarshalledKind.BLOB)
                continue;

            Marshalled ann = f.getAnnotation(Marshalled.class);

            if (kind == MarshalledKind.MAP) {
                names.add(ann.keys());
                names.add(ann.values());
            }
            else
                names.add(ann.value());
        }

        return names;
    }

    /** Direction of the field code a generator pass emits: object→wire ({@link #MARSHAL}) or wire→object ({@link #UNMARSHAL}). */
    protected enum Direction {
        /** On the way out: the code runs before the message is written. */
        OUT,

        /** On the way in: the code runs after the message is read, with cache context and class loader at hand. */
        IN
    }
}
