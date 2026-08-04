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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.QualifiedNameable;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MessageProcessor.KEY_CACHE_OBJECT_CLS;
import static org.apache.ignite.internal.MessageProcessor.MARSHALLABLE_MESSAGE_INTERFACE;

/**
 * Prints the marshalling calls of a generated wire form: the {@code @Marshalled} fields turned into their wire
 * companion fields and back — bytes, message arrays or key/value pairs — and the {@code marshal} a
 * {@code MarshallableMessage} defines. Walking the fields is {@link MessageWireFormGenerator}'s business; it asks
 * this class for the marshalling blocks of its {@code prepare} and {@code restore} methods.
 */
public class MarshallingCalls {
    /** {@code IgniteUtils} shortcut used by the generated {@code @Marshalled} handling. */
    private static final String U_CLS = "org.apache.ignite.internal.util.typedef.internal.U";

    /** Generator whose class these calls go into; supplies the imports, indent and current type. */
    private final MessageWireFormGenerator gen;

    /** */
    private final TypeMirror marshallableMsgType;

    /** {@link MarshalledKind} of each {@code @Marshalled} field; insertion-ordered so iteration gives deterministic code. */
    private final Map<VariableElement, MarshalledKind> kinds = new LinkedHashMap<>();

    /** Enclosed fields of the currently processed type. Read once per message. */
    private Map<String, VariableElement> enclosed;

    /** Whether the message marshals some of its fields itself. */
    private boolean marshallable;

    /** Whether the message has a {@code @Marshalled} field that becomes bytes. */
    private boolean hasMarshalled;

    /** */
    MarshallingCalls(MessageWireFormGenerator gen) {
        this.gen = gen;

        marshallableMsgType = gen.type(MARSHALLABLE_MESSAGE_INTERFACE);
    }

    /** Reads the {@code @Marshalled} fields of the type being generated for. Called once per message. */
    void readFields(Map<String, VariableElement> enclosed) {
        this.enclosed = enclosed;

        kinds.clear();

        for (VariableElement f : enclosed.values()) {
            MarshalledKind kind = marshalledKind(f);

            if (kind != null)
                kinds.put(f, kind);
        }

        marshallable = marshallableMsgType != null && gen.assignableFrom(gen.type.asType(), marshallableMsgType);
        hasMarshalled = kinds.values().stream().anyMatch(k -> k == MarshalledKind.BLOB || k == MarshalledKind.ELEMENT_BLOBS);
    }

    /** @return {@code true} if the message marshals anything, so the generated class needs a {@code Marshaller}. */
    boolean needsMarshaller() {
        return marshallable || hasMarshalled;
    }

    /** @return the code turning the message's {@code @Marshalled} fields into bytes, empty when it has none. */
    List<String> prepare() {
        if (!kinds.isEmpty())
            gen.imports.add(U_CLS);

        List<String> code = new ArrayList<>();

        for (VariableElement field : enclosed.values()) {
            appendElementsPrepare(code, field);
            appendMapPrepare(code, field);
            appendElementBlobsPrepare(code, field);
        }

        appendBlobPrepare(code);

        if (marshallable)
            MessageCompanionGenerator.appendBlock(code, List.of(gen.indentedLine("msg.marshal(marshaller);")));

        return code;
    }

    /** @return the code rebuilding the message's {@code @Marshalled} fields from bytes, empty when it has none. */
    List<String> restore() {
        List<String> code = new ArrayList<>();

        if (marshallable)
            MessageCompanionGenerator.appendBlock(code, List.of(gen.indentedLine("msg.unmarshal(marshaller, clsLdr);")));

        appendBlobRestore(code);

        appendElementsRestore(code);
        appendMapRestore(code);
        appendElementBlobsRestore(code);

        return code;
    }

    /** Generates the {@code Collection<byte[]>} build-up for a {@link MarshalledKind#ELEMENT_BLOBS} field in {@code prepare}. */
    private void appendElementBlobsPrepare(List<String> body, VariableElement field) {
        if (kinds.get(field) != MarshalledKind.ELEMENT_BLOBS)
            return;

        Marshalled ann = field.getAnnotation(Marshalled.class);

        String objField = "msg." + field.getSimpleName();
        String bytesField = "msg." + ann.value();

        gen.imports.add(ArrayList.class.getName());

        List<String> code = new ArrayList<>();

        code.add(gen.indentedLine("if (%s != null && %s == null) {", objField, bytesField));

        gen.indent++;

        code.add(gen.indentedLine("%s = new ArrayList<>(%s.size());", bytesField, objField));
        code.add(MessageCompanionGenerator.EMPTY);
        code.add(gen.indentedLine("for (Object e : %s)", objField));

        gen.indent++;

        code.add(gen.indentedLine("%s.add(U.marshal(marshaller, e));", bytesField));

        gen.indent--;
        gen.indent--;

        code.add(gen.indentedLine("}"));

        MessageCompanionGenerator.appendBlock(body, code);
    }

    /** Appends a {@code toArray} assignment for a {@link MarshalledKind#ELEMENTS} field, if present. */
    private void appendElementsPrepare(List<String> body, VariableElement field) {
        if (kinds.get(field) != MarshalledKind.ELEMENTS)
            return;

        Marshalled ann = field.getAnnotation(Marshalled.class);

        String colField = "msg." + field.getSimpleName();
        String arrField = "msg." + ann.value();
        String compName = arrayComponentName(requireEnclosed(ann.value()));

        List<String> code = new ArrayList<>();

        code.add(gen.indentedLine("if (%s != null && %s == null)", colField, arrField));

        gen.indent++;

        code.add(gen.indentedLine("%s = %s.toArray(new %s[0]);", arrField, colField, compName));

        gen.indent--;

        MessageCompanionGenerator.appendBlock(body, code);
    }

    /** Appends key/value array assignments for a {@link MarshalledKind#MAP} field, if present. */
    private void appendMapPrepare(List<String> body, VariableElement field) {
        if (kinds.get(field) != MarshalledKind.MAP)
            return;

        Marshalled ann = field.getAnnotation(Marshalled.class);

        String mapField = "msg." + field.getSimpleName();
        String keysField = "msg." + ann.keys();
        String valuesField = "msg." + ann.values();
        VariableElement keysEl = requireEnclosed(ann.keys());

        List<String> code = new ArrayList<>();

        code.add(gen.indentedLine("if (%s != null && %s == null) {", mapField, keysField));

        gen.indent++;

        code.addAll(keysEl.asType().getKind() == TypeKind.ARRAY
            ? mapPrepareArrayBlock(ann, mapField, keysField, keysEl, valuesField)
            : mapPrepareViewsBlock(keysField, mapField, valuesField));

        gen.indent--;

        code.add(gen.indentedLine("}"));

        MessageCompanionGenerator.appendBlock(body, code);
    }

    /** Generates {@code U.marshal} calls for the {@link MarshalledKind#BLOB} fields in {@code prepare}. */
    private void appendBlobPrepare(List<String> body) {
        forEachBlob((bytesAcc, objAcc) -> {
            List<String> code = new ArrayList<>();

            code.add(gen.indentedLine("if (%s != null && %s == null)", objAcc, bytesAcc));

            gen.indent++;

            code.add(gen.indentedLine("%s = U.marshal(marshaller, %s);", bytesAcc, objAcc));

            gen.indent--;

            return code;
        }, body);
    }

    /** Generates {@code U.unmarshal} calls for the {@link MarshalledKind#BLOB} fields in the cache-aware {@code restore}. */
    private void appendBlobRestore(List<String> body) {
        forEachBlob((bytesAcc, objAcc) -> {
            List<String> code = new ArrayList<>();

            code.add(gen.indentedLine("if (%s != null) {", bytesAcc));

            gen.indent++;

            code.add(gen.indentedLine("%s = U.unmarshal(marshaller, %s, clsLdr);", objAcc, bytesAcc));
            code.add(MessageCompanionGenerator.EMPTY);

            // Drop the serialized cache once the object is restored: keeping both the deserialized value and its bytes
            // on every received message doubles retained memory (e.g. topology history nodes) and can exhaust the heap.
            code.add(gen.indentedLine("%s = null;", bytesAcc));

            gen.indent--;

            code.add(gen.indentedLine("}"));

            return code;
        }, body);
    }

    /** Generates Set reconstruction for all {@link MarshalledKind#ELEMENTS} {@code @Marshalled} fields. */
    private void appendElementsRestore(List<String> body) {
        for (VariableElement field : enclosed.values()) {
            if (kinds.get(field) != MarshalledKind.ELEMENTS)
                continue;

            Marshalled colAnn = field.getAnnotation(Marshalled.class);

            String colField = "msg." + field.getSimpleName();
            String arrField = "msg." + colAnn.value();
            VariableElement wireField = requireEnclosed(colAnn.value());

            List<String> code = new ArrayList<>();

            code.add(gen.indentedLine("if (%s != null) {", arrField));

            gen.indent++;

            code.add(gen.indentedLine("%s = U.newHashSet(%s.length);", colField, arrField));
            code.add(MessageCompanionGenerator.EMPTY);
            code.addAll(elementsRestoreLoop(wireField, colField, arrField));
            code.add(MessageCompanionGenerator.EMPTY);
            code.add(gen.indentedLine("%s = null;", arrField));

            gen.indent--;

            code.add(gen.indentedLine("}"));

            MessageCompanionGenerator.appendBlock(body, code);
        }
    }

    /** Generates Collection reconstruction for {@link MarshalledKind#ELEMENT_BLOBS} {@code @Marshalled} fields (cache-aware pass only). */
    private void appendElementBlobsRestore(List<String> body) {
        for (VariableElement field : enclosed.values()) {
            if (kinds.get(field) != MarshalledKind.ELEMENT_BLOBS)
                continue;

            Marshalled ann = field.getAnnotation(Marshalled.class);

            String objField = "msg." + field.getSimpleName();
            String bytesField = "msg." + ann.value();

            gen.imports.add(ArrayList.class.getName());
            gen.imports.add(Map.class.getName());
            gen.imports.add(KEY_CACHE_OBJECT_CLS);

            List<String> code = new ArrayList<>();

            code.add(gen.indentedLine("if (%s != null) {", bytesField));

            gen.indent++;

            code.add(gen.indentedLine("%s = new ArrayList<>(%s.size());", objField, bytesField));
            code.add(MessageCompanionGenerator.EMPTY);
            code.add(gen.indentedLine("for (byte[] e : %s) {", bytesField));

            gen.indent++;

            code.add(gen.indentedLine("Object o = U.unmarshal(marshaller, e, clsLdr);"));
            code.add(MessageCompanionGenerator.EMPTY);
            code.add(gen.indentedLine("if (o instanceof Map.Entry) {"));

            gen.indent++;

            code.add(gen.indentedLine("Object key = ((Map.Entry<?, ?>)o).getKey();"));
            code.add(MessageCompanionGenerator.EMPTY);
            code.add(gen.indentedLine("if (key instanceof KeyCacheObject)"));

            gen.indent++;

            code.add(gen.indentedLine("((KeyCacheObject)key).unmarshal(ctx, clsLdr);"));

            gen.indent--;
            gen.indent--;

            code.add(gen.indentedLine("}"));
            code.add(MessageCompanionGenerator.EMPTY);
            code.add(gen.indentedLine("%s.add(o);", objField));

            gen.indent--;

            code.add(gen.indentedLine("}"));
            code.add(MessageCompanionGenerator.EMPTY);
            code.add(gen.indentedLine("%s = null;", bytesField));

            gen.indent--;

            code.add(gen.indentedLine("}"));

            MessageCompanionGenerator.appendBlock(body, code);
        }
    }

    /** Generates the {@code for} loop refilling the collection from its wire array. */
    private List<String> elementsRestoreLoop(VariableElement wireField, String colField, String arrField) {
        String compName = arrayComponentName(wireField);

        List<String> code = new ArrayList<>();

        code.add(gen.indentedLine("for (%s e : %s) {", compName, arrField));

        gen.indent++;

        code.add(gen.indentedLine("%s.add(e);", colField));

        gen.indent--;

        code.add(gen.indentedLine("}"));

        return code;
    }

    /** Generates Map reconstruction for all {@link MarshalledKind#MAP} fields. */
    private void appendMapRestore(List<String> body) {
        for (VariableElement field : enclosed.values()) {
            if (kinds.get(field) != MarshalledKind.MAP)
                continue;

            Marshalled ann = field.getAnnotation(Marshalled.class);

            VariableElement keysEl = requireEnclosed(ann.keys());
            VariableElement valsEl = requireEnclosed(ann.values());

            String mapField = "msg." + field.getSimpleName();
            String keysField = "msg." + ann.keys();
            String valsField = "msg." + ann.values();

            List<String> code = keysEl.asType().getKind() == TypeKind.ARRAY
                ? mapRestoreArrayBlock(field, keysEl, valsEl, mapField, keysField, valsField)
                : mapRestoreCollectionBlock(keysEl, valsEl, mapField, keysField, valsField);

            MessageCompanionGenerator.appendBlock(body, code);
        }
    }

    /** Generates indexed-loop Map reconstruction for array-backed {@link MarshalledKind#MAP} fields. */
    private List<String> mapRestoreArrayBlock(VariableElement field, VariableElement keysEl, VariableElement valsEl, String mapField,
        String keysField, String valsField) {
        List<String> code = new ArrayList<>();

        code.add(gen.indentedLine("if (%s != null) {", keysField));

        gen.indent++;

        if (!field.getModifiers().contains(Modifier.FINAL)) {
            code.add(gen.indentedLine("%s = U.newHashMap(%s.length);", mapField, keysField));
            code.add(MessageCompanionGenerator.EMPTY);
        }

        code.add(gen.indentedLine("for (int i = 0; i < %s.length; i++) {", keysField));

        gen.indent++;

        code.addAll(mapPutBlock(
            arrayComponentName(keysEl) + " k = " + keysField + "[i];",
            arrayComponentName(valsEl) + " v = " + valsField + "[i];",
            mapField));

        gen.indent--;

        code.add(gen.indentedLine("}"));
        code.add(MessageCompanionGenerator.EMPTY);
        code.add(gen.indentedLine("%s = null;", keysField));
        code.add(gen.indentedLine("%s = null;", valsField));

        gen.indent--;

        code.add(gen.indentedLine("}"));

        return code;
    }

    /** Generates iterator-based Map reconstruction for collection-backed {@link MarshalledKind#MAP} fields. */
    private List<String> mapRestoreCollectionBlock(VariableElement keysEl, VariableElement valsEl, String mapField,
        String keysField, String valsField) {
        TypeMirror keyCompType = ((DeclaredType)keysEl.asType()).getTypeArguments().get(0);
        TypeMirror valCompType = ((DeclaredType)valsEl.asType()).getTypeArguments().get(0);

        Element keyElem = gen.element(keyCompType);
        Element valElem = gen.element(valCompType);

        String keyCompName = keyElem.getSimpleName().toString();
        String valCompName = valElem.getSimpleName().toString();

        gen.imports.add(((QualifiedNameable)keyElem).getQualifiedName().toString());
        gen.imports.add(((QualifiedNameable)valElem).getQualifiedName().toString());
        gen.imports.add(Iterator.class.getName());

        List<String> code = new ArrayList<>();

        code.add(gen.indentedLine("if (%s != null) {", keysField));

        gen.indent++;

        code.add(gen.indentedLine("%s = U.newHashMap(%s.size());", mapField, keysField));
        code.add(MessageCompanionGenerator.EMPTY);
        code.add(gen.indentedLine("Iterator<%s> keyIter = %s.iterator();", keyCompName, keysField));
        code.add(gen.indentedLine("Iterator<%s> valIter = %s.iterator();", valCompName, valsField));
        code.add(MessageCompanionGenerator.EMPTY);
        code.add(gen.indentedLine("while (keyIter.hasNext()) {"));

        gen.indent++;

        code.addAll(mapPutBlock(
            keyCompName + " k = keyIter.next();",
            valCompName + " v = valIter.next();",
            mapField));

        gen.indent--;

        code.add(gen.indentedLine("}"));
        code.add(MessageCompanionGenerator.EMPTY);
        code.add(gen.indentedLine("%s = null;", keysField));
        code.add(gen.indentedLine("%s = null;", valsField));

        gen.indent--;

        code.add(gen.indentedLine("}"));

        return code;
    }

    /**
     * Generates the reconstruction-loop body shared by both map layouts: k/v declarations and {@code map.put}.
     */
    private List<String> mapPutBlock(String kDecl, String vDecl, String mapField) {
        List<String> code = new ArrayList<>();

        code.add(gen.indentedLine("%s", kDecl));
        code.add(gen.indentedLine("%s", vDecl));

        code.add(MessageCompanionGenerator.EMPTY);
        code.add(gen.indentedLine("%s.put(k, v);", mapField));

        return code;
    }

    /** Generates key/value array population from the map's entry set. */
    private List<String> mapPrepareArrayBlock(Marshalled ann, String mapField, String keysField, VariableElement keysEl, String valuesField) {
        String compName = arrayComponentName(keysEl);
        String valCompName = arrayComponentName(requireEnclosed(ann.values()));

        List<String> inner = new ArrayList<>();

        gen.imports.add(Map.class.getName());

        inner.add(gen.indentedLine("%s = new %s[%s.size()];", keysField, compName, mapField));
        inner.add(gen.indentedLine("%s = new %s[%s.length];", valuesField, valCompName, keysField));
        inner.add(gen.indentedLine("int i = 0;"));
        inner.add(gen.indentedLine("for (Map.Entry<?, ?> e : %s.entrySet()) {", mapField));

        gen.indent++;

        inner.add(gen.indentedLine("%s[i] = (%s)e.getKey();", keysField, compName));
        inner.add(gen.indentedLine("%s[i] = (%s)e.getValue();", valuesField, valCompName));
        inner.add(gen.indentedLine("i++;"));

        gen.indent--;

        inner.add(gen.indentedLine("}"));

        return inner;
    }

    /** Generates key/value assignments backed by the map's own {@code keySet()} and {@code values()} views. */
    private List<String> mapPrepareViewsBlock(String keysField, String mapField, String valuesField) {
        List<String> inner = new ArrayList<>();

        inner.add(gen.indentedLine("%s = %s.keySet();", keysField, mapField));
        inner.add(gen.indentedLine("%s = %s.values();", valuesField, mapField));

        return inner;
    }

    /** Iterates the {@link MarshalledKind#BLOB} fields and applies {@code codeGen(bytesAccessor, objAccessor)} to each. */
    private void forEachBlob(BiFunction<String, String, List<String>> codeGen, List<String> body) {
        for (VariableElement field : enclosed.values()) {
            if (kinds.get(field) != MarshalledKind.BLOB)
                continue;

            Marshalled ann = field.getAnnotation(Marshalled.class);

            MessageCompanionGenerator.appendBlock(body, codeGen.apply("msg." + ann.value(), "msg." + field.getSimpleName()));
        }
    }

    /** Returns the simple name of the array component type of {@code field}, registering its import. */
    private String arrayComponentName(VariableElement field) {
        Element comp = ((DeclaredType)((ArrayType)field.asType()).getComponentType()).asElement();

        gen.imports.add(((QualifiedNameable)comp).getQualifiedName().toString());

        return comp.getSimpleName().toString();
    }

    /** Marshalling flavour of a {@code @Marshalled} field, told apart by the shape of its companion wire field(s). */
    private enum MarshalledKind {
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
    private @Nullable MarshalledKind marshalledKind(VariableElement field) {
        Marshalled ann = field.getAnnotation(Marshalled.class);

        if (ann == null)
            return null;

        boolean map = !ann.keys().isEmpty() || !ann.values().isEmpty();

        if (map == !ann.value().isEmpty() || (map && (ann.keys().isEmpty() || ann.values().isEmpty()))) {
            gen.env.getMessager().printMessage(Diagnostic.Kind.ERROR,
                "@Marshalled must set either value() or both keys() and values()", field);

            return null;
        }

        if (map)
            return MarshalledKind.MAP;

        TypeMirror wire = requireEnclosed(ann.value()).asType();

        if (wire.getKind() == TypeKind.ARRAY) {
            return ((ArrayType)wire).getComponentType().getKind() == TypeKind.BYTE
                ? MarshalledKind.BLOB
                : MarshalledKind.ELEMENTS;
        }

        return MarshalledKind.ELEMENT_BLOBS;
    }

    /** Returns the enclosed field named {@code name}, or throws if absent. */
    private VariableElement requireEnclosed(String name) {
        VariableElement el = enclosed.get(name);

        if (el == null)
            throw new IllegalStateException("@Marshalled companion field '" + name + "' not found in " + gen.type);

        return el;
    }
}
