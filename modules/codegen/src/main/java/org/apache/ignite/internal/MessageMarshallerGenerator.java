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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.QualifiedNameable;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;

import static org.apache.ignite.internal.MessageProcessor.IGNITE_CHECKED_EXCEPTION_CLS;
import static org.apache.ignite.internal.MessageProcessor.KEY_CACHE_OBJECT_CLS;
import static org.apache.ignite.internal.MessageProcessor.MARSHALLABLE_MESSAGE_INTERFACE;

/**
 * Generates the {@code *Marshaller} class of a {@code Message}: the code that turns a {@code @Marshalled} field into
 * bytes and back, and calls the {@code marshal} a {@code MarshallableMessage} defines. Walking the fields is a step of
 * its own and belongs to {@link MessageWireFormGenerator}; a message with nothing to marshal gets no marshaller.
 */
public class MessageMarshallerGenerator extends MessageWireCompanionGenerator {
    /** Interface the generated marshallers implement. */
    private static final String MESSAGE_MARSHALLER_CLS = "org.apache.ignite.plugin.extensions.communication.MessageMarshaller";

    /** */
    private static final String MARSHALLER_CLS = "org.apache.ignite.marshaller.Marshaller";

    /** */
    private final TypeMirror marshallableMsgType;

    /** Whether the message marshals some of its fields itself. */
    private boolean marshallable;

    /** Whether the message has a {@code @Marshalled} field that becomes bytes. */
    private boolean hasMarshalled;

    /** */
    MessageMarshallerGenerator(ProcessingEnvironment env) {
        super(env);

        marshallableMsgType = type(MARSHALLABLE_MESSAGE_INTERFACE);
    }

    /** {@inheritDoc} */
    @Override protected String typeSuffix() {
        return "Marshaller";
    }

    /** {@inheritDoc} */
    @Override protected void generateBody(List<VariableElement> fields) {
        readFields();

        marshallable = marshallableMsgType != null && assignableFrom(type.asType(), marshallableMsgType);
        hasMarshalled = kinds.values().stream().anyMatch(k -> k == MarshalledKind.BLOB || k == MarshalledKind.ELEMENT_BLOBS);

        generateMarshalMethod(fields);
        generateUnmarshalMethod(fields);
    }

    /** {@inheritDoc} */
    @Override protected String buildClassCode(String clsName) throws IOException {
        if (!hasStatements)
            return null;

        try (Writer writer = new StringWriter()) {
            imports.add(type.toString());
            imports.add(MESSAGE_MARSHALLER_CLS);

            if (marshallable || hasMarshalled)
                imports.add(MARSHALLER_CLS);

            writeClassHeader(writer, "MessageMarshaller", clsName);

            writer.write(" {" + NL);

            writeConstructor(writer, clsName);

            for (String line : methods)
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

    /** Generates the {@code marshal} method: the fields that become bytes, plus the message's own call. */
    private void generateMarshalMethod(List<VariableElement> orderedFields) {
        imports.add(IGNITE_CHECKED_EXCEPTION_CLS);
        imports.add(GRID_KERNAL_CONTEXT_CLS);
        imports.add(CACHE_OBJECT_CONTEXT_CLS);

        if (!kinds.isEmpty())
            imports.add(U_CLS);

        String signature = "marshal(" + simpleNameWithGeneric(type) + " msg, GridKernalContext kctx, CacheObjectContext cacheObjCtx)";

        hasStatements |= emitMethod(methods, signature, body -> {
            usesMsgFactory = false;

            List<String> code = new ArrayList<>();

            appendMarshalledFieldsPrepare(code);
            appendMarshalledPrepare(code);

            if (marshallable)
                appendBlock(code, List.of(indentedLine("msg.marshal(marshaller);")));

            if (code.isEmpty())
                return;

            if (usesCtx(code))
                appendBlock(body, List.of(ctxResolutionLine()));

            body.addAll(code);

            prependMsgFactoryResolution(body);
        });
    }

    /** Generates the {@code unmarshal} method: the message's own call, then the fields rebuilt from bytes. */
    private void generateUnmarshalMethod(List<VariableElement> orderedFields) {
        String params = simpleNameWithGeneric(type) + " msg, GridKernalContext kctx, CacheObjectContext cacheObjCtx, ClassLoader clsLdr";

        hasStatements |= emitMethod(methods, "unmarshal(" + params + ")", body -> {
            usesMsgFactory = false;

            List<String> code = new ArrayList<>();

            if (marshallable)
                appendBlock(code, List.of(indentedLine("msg.unmarshal(marshaller, clsLdr);")));

            appendMarshalledFinish(code);

            appendMarshalledElementsFinish(code);
            appendMarshalledMapFinish(code);
            appendMarshalledElementBlobsFinish(code);

            if (code.isEmpty())
                return;

            if (usesCtx(code))
                appendBlock(body, List.of(ctxResolutionLine()));

            body.addAll(code);

            prependMsgFactoryResolution(body);
        });
    }

    /** Generates logical→wire conversions for the element-, blob-list- and map-flavoured {@code @Marshalled} fields. */
    private void appendMarshalledFieldsPrepare(List<String> body) {
        for (VariableElement field : enclosed.values()) {
            appendCollectionPrepare(body, field);
            appendMapPrepare(body, field);
            appendObjectsPrepare(body, field);
        }
    }

    /** Generates the {@code Collection<byte[]>} build-up for a per-element-blobs {@code @Marshalled} field in marshal. */
    private void appendObjectsPrepare(List<String> body, VariableElement field) {
        if (kinds.get(field) != MarshalledKind.ELEMENT_BLOBS)
            return;

        Marshalled ann = field.getAnnotation(Marshalled.class);

        String objField = "msg." + field.getSimpleName();
        String bytesField = "msg." + ann.value();

        imports.add(ArrayList.class.getName());

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

    /** Appends a {@code toArray} assignment for a per-element {@code @Marshalled} field, if present. */
    private void appendCollectionPrepare(List<String> body, VariableElement field) {
        if (kinds.get(field) != MarshalledKind.ELEMENTS)
            return;

        Marshalled ann = field.getAnnotation(Marshalled.class);

        String colField = "msg." + field.getSimpleName();
        String arrField = "msg." + ann.value();
        String compName = arrayComponentName(requireEnclosed(enclosed, ann.value(), "@Marshalled"));

        List<String> code = new ArrayList<>();

        code.add(indentedLine("if (%s != null && %s == null)", colField, arrField));

        indent++;

        code.add(indentedLine("%s = %s.toArray(new %s[0]);", arrField, colField, compName));

        indent--;

        appendBlock(body, code);
    }

    /** Appends key/value array assignments for a map-flavoured {@code @Marshalled} field, if present. */
    private void appendMapPrepare(List<String> body, VariableElement field) {
        if (kinds.get(field) != MarshalledKind.MAP)
            return;

        Marshalled ann = field.getAnnotation(Marshalled.class);

        String mapField = "msg." + field.getSimpleName();
        String keysField = "msg." + ann.keys();
        String valuesField = "msg." + ann.values();
        VariableElement keysEl = requireEnclosed(enclosed, ann.keys(), "@Marshalled");

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

    /** Generates Set reconstruction for all {@link MarshalledKind#ELEMENTS} {@code @Marshalled} fields. */
    private void appendMarshalledElementsFinish(List<String> body) {
        for (VariableElement field : enclosed.values()) {
            if (kinds.get(field) != MarshalledKind.ELEMENTS)
                continue;

            Marshalled colAnn = field.getAnnotation(Marshalled.class);

            String colField = "msg." + field.getSimpleName();
            String arrField = "msg." + colAnn.value();
            VariableElement wireField = requireEnclosed(enclosed, colAnn.value(), "@Marshalled");

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

    /** Generates Collection reconstruction for {@link MarshalledKind#ELEMENT_BLOBS} {@code @Marshalled} fields (cache-aware pass only). */
    private void appendMarshalledElementBlobsFinish(List<String> body) {
        for (VariableElement field : enclosed.values()) {
            if (kinds.get(field) != MarshalledKind.ELEMENT_BLOBS)
                continue;

            Marshalled ann = field.getAnnotation(Marshalled.class);

            String objField = "msg." + field.getSimpleName();
            String bytesField = "msg." + ann.value();

            imports.add(ArrayList.class.getName());
            imports.add(Map.class.getName());
            imports.add(KEY_CACHE_OBJECT_CLS);

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

        List<String> code = new ArrayList<>();

        code.add(indentedLine("for (%s e : %s) {", compName, arrField));

        indent++;

        code.add(indentedLine("%s.add(e);", colField));

        indent--;

        code.add(indentedLine("}"));

        return code;
    }

    /** Generates Map reconstruction for all map-flavoured {@code @Marshalled} fields. */
    private void appendMarshalledMapFinish(List<String> body) {
        for (VariableElement field : enclosed.values()) {
            if (kinds.get(field) != MarshalledKind.MAP)
                continue;

            Marshalled ann = field.getAnnotation(Marshalled.class);

            VariableElement keysEl = requireEnclosed(enclosed, ann.keys(), "@Marshalled");
            VariableElement valsEl = requireEnclosed(enclosed, ann.values(), "@Marshalled");

            String mapField = "msg." + field.getSimpleName();
            String keysField = "msg." + ann.keys();
            String valsField = "msg." + ann.values();

            List<String> code = keysEl.asType().getKind() == TypeKind.ARRAY
                ? mapFinishArrayBlock(field, keysEl, valsEl, mapField, keysField, valsField)
                : mapFinishCollectionBlock(keysEl, valsEl, mapField, keysField, valsField);

            appendBlock(body, code);
        }
    }

    /** Generates indexed-loop Map reconstruction for array-backed map-flavoured {@code @Marshalled} fields. */
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

    /** Generates iterator-based Map reconstruction for collection-backed map-flavoured {@code @Marshalled} fields. */
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
        imports.add(Iterator.class.getName());

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

        code.addAll(mapPutBlock(
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
     * Generates the reconstruction-loop body shared by both map layouts: k/v declarations,
     * element unmarshal and {@code map.put}.
     */
    private List<String> mapPutBlock(String kDecl, String vDecl, String mapField) {
        List<String> code = new ArrayList<>();

        code.add(indentedLine("%s", kDecl));
        code.add(indentedLine("%s", vDecl));

        code.add(EMPTY);
        code.add(indentedLine("%s.put(k, v);", mapField));

        return code;
    }

    /** Generates key/value array population from the map's entry set. */
    private List<String> arrayMapBody(Marshalled ann, String mapField, String keysField, VariableElement keysEl, String valuesField) {
        String compName = arrayComponentName(keysEl);
        String valCompName = arrayComponentName(requireEnclosed(enclosed, ann.values(), "@Marshalled"));

        List<String> inner = new ArrayList<>();

        imports.add(Map.class.getName());

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

    /** Iterates all {@code @Marshalled} fields and applies {@code codeGen(bytesAccessor, objAccessor)} to each. */
    private void forEachMarshalled(BiFunction<String, String, List<String>> codeGen, List<String> body) {
        for (VariableElement field : enclosed.values()) {
            if (kinds.get(field) != MarshalledKind.BLOB)
                continue;

            Marshalled ann = field.getAnnotation(Marshalled.class);

            appendBlock(body, codeGen.apply("msg." + ann.value(), "msg." + field.getSimpleName()));
        }
    }

    /** Returns the simple name of the array component type of {@code field}, registering its import. */
    private String arrayComponentName(VariableElement field) {
        Element comp = ((DeclaredType)((ArrayType)field.asType()).getComponentType()).asElement();

        imports.add(((QualifiedNameable)comp).getQualifiedName().toString());

        return comp.getSimpleName().toString();
    }

    /** Returns the element for {@code t}; for a type variable, uses its upper bound. */
    private Element element(TypeMirror t) {
        return t.getKind() == TypeKind.DECLARED
            ? ((DeclaredType)t).asElement()
            : ((DeclaredType)((TypeVariable)t).getUpperBound()).asElement();
    }

    /** @return {@code true} if the generated {@code code} refers to the cache object context, so it has to be resolved. */
    private static boolean usesCtx(List<String> code) {
        return code.stream().anyMatch(l -> l.matches(".*\\bctx\\b.*"));
    }
}
