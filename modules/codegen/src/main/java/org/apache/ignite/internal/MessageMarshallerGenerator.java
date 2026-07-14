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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
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

import static org.apache.ignite.internal.MessageProcessor.MARSHALLABLE_MESSAGE_INTERFACE;
import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;
import static org.apache.ignite.internal.MessageProcessor.NON_MARSHALLABLE_MESSAGE_INTERFACE;

/**
 * Generates {@code *Marshaller} classes for {@code Message} types that are not {@code NonMarshallableMessage}.
 */
public class MessageMarshallerGenerator extends MessageGenerator {
    /** Accumulated source lines for all generated marshal/unmarshal methods. */
    private final List<String> marshall = new ArrayList<>();

    /** */
    private final TypeMirror marshallableMsgType;

    /** */
    private final TypeMirror messageMirror;

    /** */
    private final TypeMirror cacheObjectMirror;

    /** */
    private final TypeMirror nonMarshallableMirror;

    /** */
    private final TypeMirror cacheGroupIdMsgMirror;

    /** */
    private final TypeMirror mapMirror;

    /** */
    private final TypeMirror collectionMirror;

    /** */
    private boolean marshallable;

    /** */
    private boolean hasMarshalled;

    /** Whether any generated method got a non-empty body; a marshaller without one is skipped entirely. */
    private boolean hasStatements;

    /** Enclosed fields of the currently processed type. Computed once per {@link #generateBody} call. */
    private Map<String, VariableElement> enclosed;

    /** Nesting depth of the current marshal for-loop; names loop variables {@code e}, {@code e1}, {@code e2}… */
    private int loopDepth;

    /** */
    MessageMarshallerGenerator(ProcessingEnvironment env) {
        super(env);

        marshallableMsgType = type(MARSHALLABLE_MESSAGE_INTERFACE);
        messageMirror = type(MESSAGE_INTERFACE);
        cacheObjectMirror = type("org.apache.ignite.internal.processors.cache.CacheObject");
        nonMarshallableMirror = type(NON_MARSHALLABLE_MESSAGE_INTERFACE);
        cacheGroupIdMsgMirror = type("org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage");
        mapMirror = type("java.util.Map");
        collectionMirror = type("java.util.Collection");
    }

    /** {@inheritDoc} */
    @Override String typeSuffix() {
        return "Marshaller";
    }

    /** {@inheritDoc} */
    @Override boolean shouldSkip(TypeElement type, List<VariableElement> fields) {
        return isNonMarshallableMessage(type);
    }

    /** {@inheritDoc} */
    @Override void generateBody(List<VariableElement> fields) throws Exception {
        enclosed = enclosedFields();
        marshallable = marshallableMsgType != null && assignableFrom(type.asType(), marshallableMsgType);
        hasMarshalled = enclosed.values().stream().anyMatch(f ->
            f.getAnnotation(Marshalled.class) != null || f.getAnnotation(MarshalledObjects.class) != null);

        generateMarshalMethod(fields);
        generateUnmarshalMethods(fields);
    }

    /** {@inheritDoc} */
    @Override String buildClassCode(String marshallerClsName) throws IOException {
        if (!hasStatements)
            return null;

        try (Writer writer = new StringWriter()) {
            imports.add(type.toString());
            imports.add("org.apache.ignite.plugin.extensions.communication.MessageMarshaller");

            if (marshallable || hasMarshalled)
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

    /** Writes the {@code marshaller} field and the constructor initializing it, when the marshaller is needed. */
    private void writeConstructor(Writer writer, String marshallerClsName) throws IOException {
        if (!marshallable && !hasMarshalled)
            return;

        writer.write(indentedLine(METHOD_JAVADOC));
        writer.write(NL);
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

        writer.write(indentedLine("}"));
        writer.write(NL + NL);
    }

    /** Generates the {@code marshal} method body and appends it to {@link #marshall}. */
    private void generateMarshalMethod(List<VariableElement> orderedFields) {
        imports.add("org.apache.ignite.IgniteCheckedException");
        imports.add("org.apache.ignite.internal.GridKernalContext");
        imports.add("org.apache.ignite.internal.processors.cache.CacheObjectContext");

        String signature = "marshal(" + simpleNameWithGeneric(type) + " msg, GridKernalContext kctx, CacheObjectContext cacheObjCtx)";

        hasStatements |= emitMethod(marshall, signature, body -> {
            if (needsCtx(orderedFields))
                appendBlock(body, List.of(ctxResolutionLine()));

            appendMarshalledFieldsPrepare(body);
            appendMarshalledPrepare(body);

            if (marshallable)
                appendBlock(body, List.of(indentedLine("msg.marshal(marshaller);")));

            appendFields(body, orderedFields, MarshalMode.MARSHAL);
        });
    }

    /** Generates all {@code unmarshal} overloads and appends them to {@link #marshall}. */
    private void generateUnmarshalMethods(List<VariableElement> orderedFields) {
        List<VariableElement> nioFields = new ArrayList<>();
        List<VariableElement> workerFields = new ArrayList<>();

        for (VariableElement f : orderedFields) {
            boolean nioField = isNioField(f);

            if (nioField && isMessage(f.asType()) && !nestedNeedsCtx(f.asType()))
                nioFields.add(f);
            else {
                if (nioField && !isMessage(f.asType()))
                    env.getMessager().printMessage(Diagnostic.Kind.ERROR,
                        "@NioField has no effect on non-Message field '" + f.getSimpleName() + "' of type " + f.asType(), f);
                else if (nioField)
                    env.getMessager().printMessage(Diagnostic.Kind.ERROR,
                        "@NioField field '" + f.getSimpleName() + "' of type " + f.asType() + " needs a cache object " +
                            "context to unmarshal, but the NIO thread has none; only context-free messages may be @NioField", f);

                workerFields.add(f);
            }
        }

        // U is referenced only by @Marshalled/@MarshalledObjects/@MarshalledCollection/@MarshalledMap handling.
        boolean usesU = hasMarshalled || enclosed.values().stream().anyMatch(f ->
            f.getAnnotation(MarshalledCollection.class) != null || f.getAnnotation(MarshalledMap.class) != null);

        if (usesU)
            imports.add("org.apache.ignite.internal.util.typedef.internal.U");

        String msgParam = simpleNameWithGeneric(type) + " msg, GridKernalContext kctx";

        generateUnmarshalMethod(msgParam + ", CacheObjectContext cacheObjCtx, ClassLoader clsLdr", workerFields);

        if (!nioFields.isEmpty())
            generateUnmarshalNioMethod(msgParam, nioFields);
    }

    /** Generates the cache-aware {@code unmarshal} overload: the full field set, with cache context and deployment class loader. */
    private void generateUnmarshalMethod(String params, List<VariableElement> fields) {
        hasStatements |= emitMethod(marshall, "unmarshal(" + params + ")", body -> {
            Set<String> wireFieldSkip = marshalledWireFieldsToSkip();

            if (needsCtx(fields) || !wireFieldSkip.isEmpty())
                appendBlock(body, List.of(ctxResolutionLine()));

            appendFields(body, fields, MarshalMode.UNMARSHAL, wireFieldSkip);

            if (marshallable)
                appendBlock(body, List.of(indentedLine("msg.unmarshal(marshaller, clsLdr);")));

            appendMarshalledFinish(body);

            appendMarshalledCollectionFinish(body);
            appendMarshalledMapFinish(body);
            appendMarshalledObjectsFinish(body);
        });
    }

    /** Generates the {@code unmarshalNio} method for NIO-eligible {@code @Message} fields. */
    private void generateUnmarshalNioMethod(String params, List<VariableElement> nioFields) {
        hasStatements |= emitMethod(marshall, "unmarshalNio(" + params + ")", body -> {
            for (VariableElement f : nioFields)
                appendBlock(body, unmarshalNioField(fieldAccessor(f)));
        });
    }

    /** Cache-free unmarshal of a {@code @NioField} message field on the NIO thread (no cache context available). */
    private List<String> unmarshalNioField(String accessor) {
        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null)", accessor));

        indent++;

        code.add(indentedLine("MessageMarshaller.unmarshal(kctx.messageFactory(), %s, kctx);", accessor));

        indent--;

        return code;
    }

    /** Generates logical→wire conversions for all {@code @MarshalledCollection} and {@code @MarshalledMap} fields. */
    private void appendMarshalledFieldsPrepare(List<String> body) {
        for (VariableElement field : enclosed.values()) {
            appendCollectionPrepare(body, field);
            appendMapPrepare(body, field);
            appendObjectsPrepare(body, field);
        }
    }

    /** Generates the {@code Collection<byte[]>} build-up for a {@code @MarshalledObjects} field in marshal. */
    private void appendObjectsPrepare(List<String> body, VariableElement field) {
        MarshalledObjects ann = field.getAnnotation(MarshalledObjects.class);

        if (ann == null)
            return;

        String objField = "msg." + field.getSimpleName();
        String bytesField = "msg." + ann.value();

        imports.add("java.util.ArrayList");

        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null && %s == null) {", objField, bytesField));

        indent++;

        code.add(indentedLine("%s = new ArrayList<>(%s.size());", bytesField, objField));
        code.add(EMPTY);
        code.add(indentedLine("for (Object e : %s)", objField));

        indent++;

        code.add(indentedLine("%s.add(U.marshal(marshaller, e));", bytesField));

        indent--;
        indent--;

        code.add(indentedLine("}"));

        appendBlock(body, code);
    }

    /** Appends a {@code toArray} assignment for a {@code @MarshalledCollection} field, if present. */
    private void appendCollectionPrepare(List<String> body, VariableElement field) {
        MarshalledCollection ann = field.getAnnotation(MarshalledCollection.class);

        if (ann == null)
            return;

        String colField = "msg." + field.getSimpleName();
        String arrField = "msg." + ann.value();
        String compName = arrayComponentName(requireEnclosed(enclosed, ann.value(), "@MarshalledCollection"));

        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null && %s == null)", colField, arrField));

        indent++;

        code.add(indentedLine("%s = %s.toArray(new %s[0]);", arrField, colField, compName));

        indent--;

        appendBlock(body, code);
    }

    /** Appends key/value array assignments for a {@code @MarshalledMap} field, if present. */
    private void appendMapPrepare(List<String> body, VariableElement field) {
        MarshalledMap ann = field.getAnnotation(MarshalledMap.class);

        if (ann == null)
            return;

        String mapField = "msg." + field.getSimpleName();
        String keysField = "msg." + ann.keys();
        String valuesField = "msg." + ann.values();
        VariableElement keysEl = requireEnclosed(enclosed, ann.keys(), "@MarshalledMap");

        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null && %s == null) {", mapField, keysField));

        indent++;

        code.addAll(keysEl.asType().getKind() == TypeKind.ARRAY
            ? arrayMapBody(ann, mapField, keysField, keysEl, valuesField)
            : viewBasedMapBody(keysField, mapField, valuesField));

        indent--;

        code.add(indentedLine("}"));

        appendBlock(body, code);
    }

    /** Generates {@code U.marshal} calls for all {@code @Marshalled} fields in marshal. */
    private void appendMarshalledPrepare(List<String> body) {
        forEachMarshalled((bytesAcc, objAcc) -> {
            List<String> code = new ArrayList<>();

            code.add(indentedLine("if (%s != null && %s == null)", objAcc, bytesAcc));

            indent++;

            code.add(indentedLine("%s = U.marshal(marshaller, %s);", bytesAcc, objAcc));

            indent--;

            return code;
        }, body);
    }

    /** Generates {@code U.unmarshal} calls for all {@code @Marshalled} fields in the cache-aware unmarshal. */
    private void appendMarshalledFinish(List<String> body) {
        forEachMarshalled((bytesAcc, objAcc) -> {
            List<String> code = new ArrayList<>();

            code.add(indentedLine("if (%s != null) {", bytesAcc));

            indent++;

            code.add(indentedLine("%s = U.unmarshal(marshaller, %s, clsLdr);", objAcc, bytesAcc));
            code.add(EMPTY);

            // Drop the serialized cache once the object is restored: keeping both the deserialized value and its bytes
            // on every received message doubles retained memory (e.g. topology history nodes) and can exhaust the heap.
            code.add(indentedLine("%s = null;", bytesAcc));

            indent--;

            code.add(indentedLine("}"));

            return code;
        }, body);
    }

    /** Generates Set reconstruction for all {@code @MarshalledCollection} fields. */
    private void appendMarshalledCollectionFinish(List<String> body) {
        for (VariableElement field : enclosed.values()) {
            MarshalledCollection colAnn = field.getAnnotation(MarshalledCollection.class);

            if (colAnn == null)
                continue;

            String colField = "msg." + field.getSimpleName();
            String arrField = "msg." + colAnn.value();
            VariableElement wireField = requireEnclosed(enclosed, colAnn.value(), "@MarshalledCollection");

            List<String> code = new ArrayList<>();

            code.add(indentedLine("if (%s != null) {", arrField));

            indent++;

            code.add(indentedLine("%s = U.newHashSet(%s.length);", colField, arrField));
            code.add(EMPTY);
            code.addAll(collectionFinishForBlock(wireField, colField, arrField, field.getSimpleName().toString()));
            code.add(EMPTY);
            code.add(indentedLine("%s = null;", arrField));

            indent--;

            code.add(indentedLine("}"));

            appendBlock(body, code);
        }
    }

    /** Generates Collection reconstruction for all {@code @MarshalledObjects} fields (cache-aware pass only). */
    private void appendMarshalledObjectsFinish(List<String> body) {
        for (VariableElement field : enclosed.values()) {
            MarshalledObjects ann = field.getAnnotation(MarshalledObjects.class);

            if (ann == null)
                continue;

            String objField = "msg." + field.getSimpleName();
            String bytesField = "msg." + ann.value();

            imports.add("java.util.ArrayList");
            imports.add("java.util.Map");
            imports.add("org.apache.ignite.internal.processors.cache.KeyCacheObject");

            List<String> code = new ArrayList<>();

            code.add(indentedLine("if (%s != null) {", bytesField));

            indent++;

            code.add(indentedLine("%s = new ArrayList<>(%s.size());", objField, bytesField));
            code.add(EMPTY);
            code.add(indentedLine("for (byte[] e : %s) {", bytesField));

            indent++;

            code.add(indentedLine("Object o = U.unmarshal(marshaller, e, clsLdr);"));
            code.add(EMPTY);
            code.add(indentedLine("if (o instanceof Map.Entry) {"));

            indent++;

            code.add(indentedLine("Object key = ((Map.Entry<?, ?>)o).getKey();"));
            code.add(EMPTY);
            code.add(indentedLine("if (key instanceof KeyCacheObject)"));

            indent++;

            code.add(indentedLine("((KeyCacheObject)key).unmarshal(ctx, clsLdr);"));

            indent--;
            indent--;

            code.add(indentedLine("}"));
            code.add(EMPTY);
            code.add(indentedLine("%s.add(o);", objField));

            indent--;

            code.add(indentedLine("}"));
            code.add(EMPTY);
            code.add(indentedLine("%s = null;", bytesField));

            indent--;

            code.add(indentedLine("}"));

            appendBlock(body, code);
        }
    }

    /** Generates the {@code for} loop body: per-element unmarshal + try/catch add into the collection. */
    private List<String> collectionFinishForBlock(VariableElement wireField, String colField, String arrField, String fieldName) {
        String compName = arrayComponentName(wireField);
        TypeMirror compType = ((ArrayType)wireField.asType()).getComponentType();

        List<String> code = new ArrayList<>();

        code.add(indentedLine("for (%s e : %s) {", compName, arrField));

        indent++;

        code.addAll(codeFor(compType, "e", MarshalMode.UNMARSHAL));
        code.add(EMPTY);
        code.add(indentedLine("%s.add(e);", colField));

        indent--;

        code.add(indentedLine("}"));

        return code;
    }

    /** Returns names of wire fields skipped by {@link #appendFields} in UNMARSHAL mode. */
    private Set<String> marshalledWireFieldsToSkip() {
        Set<String> names = new HashSet<>();

        for (VariableElement f : enclosed.values()) {
            MarshalledCollection colAnn = f.getAnnotation(MarshalledCollection.class);
            if (colAnn != null)
                names.add(colAnn.value());

            MarshalledMap mapAnn = f.getAnnotation(MarshalledMap.class);
            if (mapAnn != null) {
                names.add(mapAnn.keys());
                names.add(mapAnn.values());
            }

            MarshalledObjects objAnn = f.getAnnotation(MarshalledObjects.class);
            if (objAnn != null)
                names.add(objAnn.value());
        }

        return names;
    }

    /** Generates Map reconstruction for all {@code @MarshalledMap} fields. */
    private void appendMarshalledMapFinish(List<String> body) {
        for (VariableElement field : enclosed.values()) {
            MarshalledMap ann = field.getAnnotation(MarshalledMap.class);

            if (ann == null)
                continue;

            VariableElement keysEl = requireEnclosed(enclosed, ann.keys(), "@MarshalledMap");
            VariableElement valsEl = requireEnclosed(enclosed, ann.values(), "@MarshalledMap");

            String mapField = "msg." + field.getSimpleName();
            String keysField = "msg." + ann.keys();
            String valsField = "msg." + ann.values();

            List<String> code = keysEl.asType().getKind() == TypeKind.ARRAY
                ? mapFinishArrayBlock(field, keysEl, valsEl, mapField, keysField, valsField)
                : mapFinishCollectionBlock(keysEl, valsEl, mapField, keysField, valsField);

            appendBlock(body, code);
        }
    }

    /** Generates indexed-loop Map reconstruction for array-backed {@code @MarshalledMap} fields. */
    private List<String> mapFinishArrayBlock(VariableElement field, VariableElement keysEl, VariableElement valsEl, String mapField,
        String keysField, String valsField) {
        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null) {", keysField));

        indent++;

        if (!field.getModifiers().contains(Modifier.FINAL)) {
            code.add(indentedLine("%s = U.newHashMap(%s.length);", mapField, keysField));
            code.add(EMPTY);
        }

        code.add(indentedLine("for (int i = 0; i < %s.length; i++) {", keysField));

        indent++;

        code.addAll(mapPutBlock(
            ((ArrayType)keysEl.asType()).getComponentType(),
            ((ArrayType)valsEl.asType()).getComponentType(),
            arrayComponentName(keysEl) + " k = " + keysField + "[i];",
            arrayComponentName(valsEl) + " v = " + valsField + "[i];",
            mapField));

        indent--;

        code.add(indentedLine("}"));
        code.add(EMPTY);
        code.add(indentedLine("%s = null;", keysField));
        code.add(indentedLine("%s = null;", valsField));

        indent--;

        code.add(indentedLine("}"));

        return code;
    }

    /** Generates iterator-based Map reconstruction for collection-backed {@code @MarshalledMap} fields. */
    private List<String> mapFinishCollectionBlock(VariableElement keysEl, VariableElement valsEl, String mapField,
        String keysField, String valsField) {
        TypeMirror keyCompType = ((DeclaredType)keysEl.asType()).getTypeArguments().get(0);
        TypeMirror valCompType = ((DeclaredType)valsEl.asType()).getTypeArguments().get(0);

        Element keyElem = element(keyCompType);
        Element valElem = element(valCompType);

        String keyCompName = keyElem.getSimpleName().toString();
        String valCompName = valElem.getSimpleName().toString();

        imports.add(((QualifiedNameable)keyElem).getQualifiedName().toString());
        imports.add(((QualifiedNameable)valElem).getQualifiedName().toString());
        imports.add("java.util.Iterator");

        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null) {", keysField));

        indent++;

        code.add(indentedLine("%s = U.newHashMap(%s.size());", mapField, keysField));
        code.add(EMPTY);
        code.add(indentedLine("Iterator<%s> keyIter = %s.iterator();", keyCompName, keysField));
        code.add(indentedLine("Iterator<%s> valIter = %s.iterator();", valCompName, valsField));
        code.add(EMPTY);
        code.add(indentedLine("while (keyIter.hasNext()) {"));

        indent++;

        code.addAll(mapPutBlock(keyCompType, valCompType,
            keyCompName + " k = keyIter.next();",
            valCompName + " v = valIter.next();",
            mapField));

        indent--;

        code.add(indentedLine("}"));
        code.add(EMPTY);
        code.add(indentedLine("%s = null;", keysField));
        code.add(indentedLine("%s = null;", valsField));

        indent--;

        code.add(indentedLine("}"));

        return code;
    }

    /**
     * Generates the reconstruction-loop body shared by both {@code @MarshalledMap} layouts: k/v declarations,
     * element unmarshal and {@code map.put}.
     */
    private List<String> mapPutBlock(TypeMirror keyCompType, TypeMirror valCompType, String kDecl, String vDecl, String mapField) {
        List<String> code = new ArrayList<>();

        code.add(indentedLine("%s", kDecl));
        code.add(indentedLine("%s", vDecl));

        List<String> keyUnmarshal = codeFor(keyCompType, "k", MarshalMode.UNMARSHAL);
        List<String> valUnmarshal = codeFor(valCompType, "v", MarshalMode.UNMARSHAL);

        if (!keyUnmarshal.isEmpty()) {
            code.add(EMPTY);
            code.addAll(keyUnmarshal);
        }

        if (!valUnmarshal.isEmpty()) {
            code.add(EMPTY);
            code.addAll(valUnmarshal);
        }

        code.add(EMPTY);
        code.add(indentedLine("%s.put(k, v);", mapField));

        return code;
    }

    /** Generates key/value array population from the map's entry set. */
    private List<String> arrayMapBody(MarshalledMap ann, String mapField, String keysField, VariableElement keysEl, String valuesField) {
        String compName = arrayComponentName(keysEl);
        String valCompName = arrayComponentName(requireEnclosed(enclosed, ann.values(), "@MarshalledMap"));

        List<String> inner = new ArrayList<>();

        imports.add("java.util.Map");

        inner.add(indentedLine("%s = new %s[%s.size()];", keysField, compName, mapField));
        inner.add(indentedLine("%s = new %s[%s.length];", valuesField, valCompName, keysField));
        inner.add(indentedLine("int i = 0;"));
        inner.add(indentedLine("for (Map.Entry<?, ?> e : %s.entrySet()) {", mapField));

        indent++;

        inner.add(indentedLine("%s[i] = (%s)e.getKey();", keysField, compName));
        inner.add(indentedLine("%s[i] = (%s)e.getValue();", valuesField, valCompName));
        inner.add(indentedLine("i++;"));

        indent--;

        inner.add(indentedLine("}"));

        return inner;
    }

    /** Generates key/value assignments backed by the map's own {@code keySet()} and {@code values()} views. */
    private List<String> viewBasedMapBody(String keysField, String mapField, String valuesField) {
        List<String> inner = new ArrayList<>();

        inner.add(indentedLine("%s = %s.keySet();", keysField, mapField));
        inner.add(indentedLine("%s = %s.values();", valuesField, mapField));

        return inner;
    }

    /** Marshals each field and appends non-empty results to {@code body}. */
    private void appendFields(List<String> body, List<VariableElement> fields, MarshalMode mode) {
        appendFields(body, fields, mode, Set.of());
    }

    /** Marshals each field, skipping names in {@code skip}, and appends non-empty results to {@code body}. */
    private void appendFields(List<String> body, List<VariableElement> fields, MarshalMode mode, Set<String> skip) {
        for (VariableElement field : fields) {
            if (skip.contains(field.getSimpleName().toString()))
                continue;

            List<String> result = codeFor(field.asType(), fieldAccessor(field), mode);

            if (!result.isEmpty())
                appendBlock(body, result);
        }
    }

    /** Returns generated marshal/unmarshal code lines for field of type {@code t}, or empty if none needed. */
    private List<String> codeFor(TypeMirror t, String accessor, MarshalMode mode) {
        if (t.getKind() == TypeKind.ARRAY) {
            TypeMirror comp = ((ArrayType)t).getComponentType();

            return comp.getKind() == TypeKind.DECLARED ? marshallArray(comp, accessor, mode) : List.of();
        }

        if (t.getKind() == TypeKind.DECLARED || t.getKind() == TypeKind.TYPEVAR) {
            if (isMessage(t))
                return isNonMarshallable(t) ? List.of() : marshallMessage(accessor, mode);
            if (isCacheObject(t))
                return marshallCacheObject(accessor, mode);
            if (isMap(t))
                return marshallMap((DeclaredType)t, accessor, mode);
            if (isCollection(t))
                return marshallCollection((DeclaredType)t, accessor, mode);
        }

        return List.of();
    }

    /** Generates a null-guarded {@code MessageMarshaller.marshal/unmarshal} call. */
    private List<String> marshallMessage(String accessor, MarshalMode mode) {
        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null)", accessor));

        indent++;

        code.add(mode == MarshalMode.MARSHAL
            ? indentedLine("MessageMarshaller.marshal(kctx.messageFactory(), %s, kctx, ctx);", accessor)
            : indentedLine("MessageMarshaller.unmarshal(kctx.messageFactory(), %s, kctx, ctx, clsLdr);", accessor));

        indent--;

        return code;
    }

    /** Generates a null-and-ctx-guarded {@code marshal/unmarshal} call on a {@code CacheObject} (marshal or cache-aware unmarshal only). */
    private List<String> marshallCacheObject(String accessor, MarshalMode mode) {
        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null && ctx != null)", accessor));

        indent++;

        code.add(mode == MarshalMode.MARSHAL
            ? indentedLine("%s.marshal(ctx);", accessor)
            : indentedLine("%s.unmarshal(ctx, clsLdr);", accessor));

        indent--;

        return code;
    }

    /** Generates a null-guarded for-each loop over the array's elements. */
    private List<String> marshallArray(TypeMirror comp, String accessor, MarshalMode mode) {
        Element elem = ((DeclaredType)comp).asElement();

        imports.add(((QualifiedNameable)elem).getQualifiedName().toString());

        indent++;

        List<String> loopCode = forLoop(elem.getSimpleName().toString(), comp, accessor, mode);

        indent--;

        return wrapNullGuarded(accessor, loopCode);
    }

    /** Generates a null-guarded for-each loop over the collection's elements. */
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

    /** Returns empty if {@code elemType} requires no marshalling; otherwise returns a for-each loop over {@code iterable}. */
    private List<String> forLoop(String typeName, TypeMirror elemType, String iterable, MarshalMode mode) {
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

    /**
     * Returns the {@code CacheObjectContext ctx} resolution line for the current message type. Cache messages resolve
     * via the cache, group messages via the cache group — the group's context outlives the stop of individual caches,
     * so cache objects still unmarshal while a cache (group) is being destroyed.
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

    /** Returns {@code true} if any field requires {@code ctx} in generated marshal/unmarshal code. */
    private boolean needsCtx(List<VariableElement> fields) {
        return fields.stream().anyMatch(f -> needsCtxType(f.asType()));
    }

    /**
     * Returns whether the {@code @Order} fields of {@code msgType} need a cache object context to unmarshal. Such a
     * message must not be a {@code @NioField}: its {@code unmarshalNio} runs on the NIO thread, which has no context.
     */
    private boolean nestedNeedsCtx(TypeMirror msgType) {
        Element el = env.getTypeUtils().asElement(msgType);

        if (!(el instanceof TypeElement))
            return false;

        List<VariableElement> ordered = new ArrayList<>();

        SystemViewRowAttributeWalkerProcessor.superclasses(env, (TypeElement)el).forEach(c -> {
            for (VariableElement f : ElementFilter.fieldsIn(c.getEnclosedElements())) {
                if (f.getAnnotation(Order.class) != null)
                    ordered.add(f);
            }
        });

        return needsCtx(ordered);
    }

    /** Returns {@code true} if type {@code t} (or its element/key/value types) requires {@code ctx}. */
    private boolean needsCtxType(TypeMirror t) {
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
    private boolean isMessage(TypeMirror type) {
        return assignableFrom(type, messageMirror);
    }

    /** */
    private boolean isCacheObject(TypeMirror type) {
        return assignableFrom(type, cacheObjectMirror);
    }

    /** Returns {@code true} if {@code type} (erased) is assignable to {@code java.util.Map}. */
    private boolean isMap(TypeMirror type) {
        return assignableFrom(erasedType(type), mapMirror);
    }

    /** Returns {@code true} if {@code type} (erased) is assignable to {@code java.util.Collection}. */
    private boolean isCollection(TypeMirror type) {
        return assignableFrom(erasedType(type), collectionMirror);
    }

    /** */
    private boolean isNonMarshallableMessage(TypeElement te) {
        return isNonMarshallable(te.asType());
    }

    /** Recursion skip for such fields is subtype-safe: subclasses inherit the {@code NonMarshallableMessage} marker. */
    private boolean isNonMarshallable(TypeMirror t) {
        return assignableFrom(t, nonMarshallableMirror);
    }

    /** */
    private boolean isCacheGroupIdMessage(TypeElement te) {
        return assignableFrom(te.asType(), cacheGroupIdMsgMirror);
    }

    /** */
    private static boolean isNioField(VariableElement field) {
        return field.getAnnotation(NioField.class) != null;
    }

    /** Returns the enclosed field named {@code name}, or throws if absent. */
    private VariableElement requireEnclosed(Map<String, VariableElement> enclosed, String name, String annotationName) {
        VariableElement el = enclosed.get(name);

        if (el == null)
            throw new IllegalStateException(annotationName + " companion field '" + name + "' not found in " + type);

        return el;
    }

    /** Iterates all {@code @Marshalled} fields and applies {@code codeGen(bytesAccessor, objAccessor)} to each. */
    private void forEachMarshalled(BiFunction<String, String, List<String>> codeGen, List<String> body) {
        for (VariableElement field : enclosed.values()) {
            Marshalled ann = field.getAnnotation(Marshalled.class);

            if (ann == null)
                continue;

            appendBlock(body, codeGen.apply("msg." + ann.value(), "msg." + field.getSimpleName()));
        }
    }

    /** Returns the element for {@code t}; for a type variable, uses its upper bound. */
    private Element element(TypeMirror t) {
        return t.getKind() == TypeKind.DECLARED ?
            ((DeclaredType)t).asElement() :
            ((DeclaredType)((TypeVariable)t).getUpperBound()).asElement();
    }

    /** Returns the simple name of the array component type of {@code field}. */
    private static String arrayComponentName(VariableElement field) {
        return ((DeclaredType)((ArrayType)field.asType()).getComponentType()).asElement().getSimpleName().toString();
    }

    /** Direction of the field code a generator pass emits: object→wire ({@link #MARSHAL}) or wire→object ({@link #UNMARSHAL}). */
    private enum MarshalMode {
        /** Marshal: object → wire bytes, on the sending side. */
        MARSHAL,

        /** Unmarshal: wire bytes → object, with full cache context and class loader. */
        UNMARSHAL
    }
}
