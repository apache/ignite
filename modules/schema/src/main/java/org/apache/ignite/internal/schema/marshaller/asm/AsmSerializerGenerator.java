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

package org.apache.ignite.internal.schema.marshaller.asm;

import com.facebook.presto.bytecode.Access;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.ClassGenerator;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.control.TryCatch;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.processing.Generated;
import jdk.jfr.Experimental;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.RowAssembler;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.AbstractSerializer;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.marshaller.SerializationException;
import org.apache.ignite.internal.schema.marshaller.Serializer;
import org.apache.ignite.internal.schema.marshaller.SerializerFactory;
import org.apache.ignite.internal.util.ObjectFactory;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;

/**
 * {@link Serializer} code generator.
 */
@Experimental
public class AsmSerializerGenerator implements SerializerFactory {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(AsmSerializerGenerator.class);

    /** Serializer package name. */
    public static final String SERIALIZER_PACKAGE_NAME = "org.apache.ignite.internal.schema.marshaller";

    /** Serializer package name prefix. */
    public static final String SERIALIZER_CLASS_NAME_PREFIX = "SerializerForSchema_";

    /** Dump generated code. */
    private final boolean dumpCode = LOG.isTraceEnabled();

    /** {@inheritDoc} */
    @Override public Serializer create(
        SchemaDescriptor schema,
        Class<?> keyClass,
        Class<?> valClass
    ) {
        final String className = SERIALIZER_CLASS_NAME_PREFIX + schema.version();

        final StringWriter writer = new StringWriter();
        try {
            // Generate Serializer code.
            long generation = System.nanoTime();

            final ClassDefinition classDef = generateSerializerClass(className, schema, keyClass, valClass);

            long compilationTime = System.nanoTime();
            generation = compilationTime - generation;

            final ClassGenerator generator = ClassGenerator.classGenerator(getClassLoader());

            if (dumpCode) {
                generator.outputTo(writer)
                    .fakeLineNumbers(true)
                    .runAsmVerifier(true)
                    .dumpRawBytecode(true);
            }

            final Class<? extends Serializer> aClass = generator.defineClass(classDef, Serializer.class);

            compilationTime = System.nanoTime() - compilationTime;

            if (LOG.isTraceEnabled()) {
                LOG.trace("ASM serializer created: codeGenStage=" + TimeUnit.NANOSECONDS.toMicros(generation) + "us" +
                    ", compileStage=" + TimeUnit.NANOSECONDS.toMicros(compilationTime) + "us." + "Code: " + writer.toString());
            }
            else if (LOG.isDebugEnabled()) {
                LOG.debug("ASM serializer created: codeGenStage=" + TimeUnit.NANOSECONDS.toMicros(generation) + "us" +
                    ", compileStage=" + TimeUnit.NANOSECONDS.toMicros(compilationTime) + "us.");
            }

            // Instantiate serializer.
            return aClass
                .getDeclaredConstructor(
                    SchemaDescriptor.class,
                    ObjectFactory.class,
                    ObjectFactory.class)
                .newInstance(
                    schema,
                    MarshallerUtil.factoryForClass(keyClass),
                    MarshallerUtil.factoryForClass(valClass));

        }
        catch (Exception | LinkageError e) {
            throw new IgniteInternalException("Failed to create serializer for key-value pair: schemaVer=" + schema.version() +
                ", keyClass=" + keyClass.getSimpleName() + ", valueClass=" + valClass.getSimpleName(), e);
        }
    }

    /**
     * Generates serializer class definition.
     *
     * @param className Serializer class name.
     * @param schema Schema descriptor.
     * @param keyClass Key class.
     * @param valClass Value class.
     * @return Generated java class definition.
     */
    private ClassDefinition generateSerializerClass(
        String className,
        SchemaDescriptor schema,
        Class<?> keyClass,
        Class<?> valClass
    ) {
        MarshallerCodeGenerator keyMarsh = createMarshaller(keyClass, schema.keyColumns(), 0);
        MarshallerCodeGenerator valMarsh = createMarshaller(valClass, schema.valueColumns(), schema.keyColumns().length());

        final ClassDefinition classDef = new ClassDefinition(
            EnumSet.of(Access.PUBLIC),
            SERIALIZER_PACKAGE_NAME.replace('.', '/') + '/' + className,
            ParameterizedType.type(AbstractSerializer.class)
        );

        classDef.declareAnnotation(Generated.class).setValue("value", getClass().getCanonicalName());

        final FieldDefinition keyClassField = classDef.declareField(EnumSet.of(Access.PRIVATE, Access.STATIC, Access.FINAL),
            "KEY_CLASS", Class.class);
        final FieldDefinition valueClassField = classDef.declareField(EnumSet.of(Access.PRIVATE, Access.STATIC, Access.FINAL),
            "VALUE_CLASS", Class.class);

        keyMarsh.initStaticHandlers(classDef, keyClassField);
        valMarsh.initStaticHandlers(classDef, valueClassField);

        generateFieldsAndConstructor(classDef);
        generateAssemblerFactoryMethod(classDef, schema, keyMarsh, valMarsh);

        generateSerializeMethod(classDef, keyMarsh, valMarsh);
        generateDeserializeKeyMethod(classDef, keyMarsh);
        generateDeserializeValueMethod(classDef, valMarsh);

        return classDef;
    }

    /**
     * Creates marshaller code generator for given class.
     *
     * @param tClass Target class.
     * @param columns Columns that tClass mapped to.
     * @param firstColIdx First column absolute index in schema.
     * @return Marshaller code generator.
     */
    private static MarshallerCodeGenerator createMarshaller(
        Class<?> tClass,
        Columns columns,
        int firstColIdx
    ) {
        final BinaryMode mode = MarshallerUtil.mode(tClass);

        if (mode == null)
            return new ObjectMarshallerCodeGenerator(columns, tClass, firstColIdx);
        else
            return new IdentityMarshallerCodeGenerator(tClass, ColumnAccessCodeGenerator.createAccessor(mode, firstColIdx));
    }

    /**
     * Generates fields and constructor.
     *
     * @param classDef Serializer class definition.
     */
    private void generateFieldsAndConstructor(ClassDefinition classDef) {
        classDef.declareField(EnumSet.of(Access.PRIVATE, Access.FINAL), "keyFactory", ParameterizedType.type(ObjectFactory.class));
        classDef.declareField(EnumSet.of(Access.PRIVATE, Access.FINAL), "valFactory", ParameterizedType.type(ObjectFactory.class));

        final MethodDefinition constrDef = classDef.declareConstructor(
            EnumSet.of(Access.PUBLIC),
            Parameter.arg("schema", SchemaDescriptor.class),
            Parameter.arg("keyFactory", ParameterizedType.type(ObjectFactory.class)),
            Parameter.arg("valFactory", ParameterizedType.type(ObjectFactory.class))
        );

        constrDef.getBody()
            .append(constrDef.getThis())
            .append(constrDef.getScope().getVariable("schema"))
            .invokeConstructor(classDef.getSuperClass(), ParameterizedType.type(SchemaDescriptor.class))
            .append(constrDef.getThis().setField("keyFactory", constrDef.getScope().getVariable("keyFactory")))
            .append(constrDef.getThis().setField("valFactory", constrDef.getScope().getVariable("valFactory")))
            .ret();
    }

    /**
     * Generates helper method.
     *
     * @param classDef Serializer class definition.
     * @param schema Schema descriptor.
     * @param keyMarsh Key marshaller code generator.
     * @param valMarsh Value marshaller code generator.
     */
    private void generateAssemblerFactoryMethod(
        ClassDefinition classDef,
        SchemaDescriptor schema,
        MarshallerCodeGenerator keyMarsh,
        MarshallerCodeGenerator valMarsh
    ) {
        final MethodDefinition methodDef = classDef.declareMethod(
            EnumSet.of(Access.PROTECTED),
            "createAssembler",
            ParameterizedType.type(RowAssembler.class),
            Parameter.arg("key", Object.class),
            Parameter.arg("val", Object.class)
        );
        methodDef.declareAnnotation(Override.class);

        final Scope scope = methodDef.getScope();
        final BytecodeBlock body = methodDef.getBody();

        final Variable varlenKeyCols = scope.declareVariable("varlenKeyCols", body, BytecodeExpressions.defaultValue(int.class));
        final Variable varlenValueCols = scope.declareVariable("varlenValueCols", body, BytecodeExpressions.defaultValue(int.class));
        final Variable varlenKeyColsSize = scope.declareVariable("varlenKeyColsSize", body, BytecodeExpressions.defaultValue(int.class));
        final Variable varlenValueColsSize = scope.declareVariable("varlenValueColsSize", body, BytecodeExpressions.defaultValue(int.class));

        final Variable keyCols = scope.declareVariable(Columns.class, "keyCols");
        final Variable valCols = scope.declareVariable(Columns.class, "valCols");

        body.append(keyCols.set(
            methodDef.getThis().getField("schema", SchemaDescriptor.class)
                .invoke("keyColumns", Columns.class)));
        body.append(valCols.set(
            methodDef.getThis().getField("schema", SchemaDescriptor.class)
                .invoke("valueColumns", Columns.class)));

        Columns columns = schema.keyColumns();
        if (columns.firstVarlengthColumn() >= 0) {
            final Variable tmp = scope.createTempVariable(Object.class);

            for (int i = columns.firstVarlengthColumn(); i < columns.length(); i++) {
                assert !columns.column(i).type().spec().fixedLength();

                body.append(keyMarsh.getValue(classDef.getType(), scope.getVariable("key"), i)).putVariable(tmp);
                body.append(new IfStatement().condition(BytecodeExpressions.isNotNull(tmp)).ifTrue(
                    new BytecodeBlock()
                        .append(varlenKeyCols.increment())
                        .append(BytecodeExpressions.add(
                            varlenKeyColsSize,
                            getColumnValueSize(tmp, keyCols, i))
                        )
                        .putVariable(varlenKeyColsSize))
                );
            }
        }

        columns = schema.valueColumns();
        if (columns.firstVarlengthColumn() >= 0) {
            final Variable tmp = scope.createTempVariable(Object.class);

            for (int i = columns.firstVarlengthColumn(); i < columns.length(); i++) {
                assert !columns.column(i).type().spec().fixedLength();

                body.append(valMarsh.getValue(classDef.getType(), scope.getVariable("val"), i)).putVariable(tmp);
                body.append(new IfStatement().condition(BytecodeExpressions.isNotNull(tmp)).ifTrue(
                    new BytecodeBlock()
                        .append(varlenValueCols.increment())
                        .append(BytecodeExpressions.add(
                            varlenValueColsSize,
                            getColumnValueSize(tmp, valCols, i))
                        )
                        .putVariable(varlenValueColsSize))
                );
            }
        }

        body.append(BytecodeExpressions.newInstance(RowAssembler.class,
            methodDef.getThis().getField("schema", SchemaDescriptor.class),
            BytecodeExpressions.invokeStatic(RowAssembler.class, "rowSize", int.class,
                keyCols, varlenKeyCols, varlenKeyColsSize,
                valCols, varlenValueCols, varlenValueColsSize),
            varlenKeyCols,
            varlenValueCols));

        body.retObject();
    }

    /**
     * Generates serialize method.
     *
     * @param classDef Serializer class definition.
     * @param keyMarsh Key marshaller code generator.
     * @param valMarsh Value marshaller code generator.
     */
    private void generateSerializeMethod(
        ClassDefinition classDef,
        MarshallerCodeGenerator keyMarsh,
        MarshallerCodeGenerator valMarsh
    ) {
        final MethodDefinition methodDef = classDef.declareMethod(
            EnumSet.of(Access.PROTECTED),
            "serialize0",
            ParameterizedType.type(byte[].class),
            Parameter.arg("asm", RowAssembler.class),
            Parameter.arg("key", Object.class),
            Parameter.arg("val", Object.class)
        ).addException(SerializationException.class);

        methodDef.declareAnnotation(Override.class);

        final Variable asm = methodDef.getScope().getVariable("asm");

        methodDef.getBody().append(new IfStatement().condition(BytecodeExpressions.isNull(asm)).ifTrue(
            new BytecodeBlock()
                .append(BytecodeExpressions.newInstance(IgniteInternalException.class, BytecodeExpressions.constantString("ASM can't be null.")))
                .throwObject()
        ));

        final BytecodeBlock block = new BytecodeBlock();
        block.append(
            keyMarsh.marshallObject(
                classDef.getType(),
                asm,
                methodDef.getScope().getVariable("key"))
        )
            .append(
                valMarsh.marshallObject(
                    classDef.getType(),
                    asm,
                    methodDef.getScope().getVariable("val"))
            )
            .append(asm.invoke("build", byte[].class))
            .retObject();

        final Variable ex = methodDef.getScope().createTempVariable(Throwable.class);
        methodDef.getBody().append(new TryCatch(
            block,
            new BytecodeBlock()
                .putVariable(ex)
                .append(BytecodeExpressions.newInstance(SerializationException.class, ex))
                .throwObject(),
            ParameterizedType.type(Throwable.class)
        ));

    }

    /**
     * Generates deserialize method.
     *
     * @param classDef Serializer class definition.
     * @param keyMarsh Key marshaller code generator.
     */
    private void generateDeserializeKeyMethod(ClassDefinition classDef, MarshallerCodeGenerator keyMarsh) {
        final MethodDefinition methodDef = classDef.declareMethod(
            EnumSet.of(Access.PROTECTED),
            "deserializeKey0",
            ParameterizedType.type(Object.class),
            Parameter.arg("row", Row.class)
        ).addException(SerializationException.class);

        methodDef.declareAnnotation(Override.class);

        final Variable obj = methodDef.getScope().declareVariable(Object.class, "obj");

        if (!keyMarsh.isSimpleType())
            methodDef.getBody().append(obj.set(methodDef.getThis().getField("keyFactory", ObjectFactory.class)
                .invoke("create", Object.class)));

        methodDef.getBody()
            .append(keyMarsh.unmarshallObject(classDef.getType(), methodDef.getScope().getVariable("row"), obj))
            .append(obj)
            .retObject();
    }

    /**
     * Generates serialize method.
     *
     * @param classDef Serializer class definition.
     * @param valMarsh Value marshaller code generator.
     */
    private void generateDeserializeValueMethod(ClassDefinition classDef, MarshallerCodeGenerator valMarsh) {
        final MethodDefinition methodDef = classDef.declareMethod(
            EnumSet.of(Access.PROTECTED),
            "deserializeValue0",
            ParameterizedType.type(Object.class),
            Parameter.arg("row", Row.class)
        ).addException(SerializationException.class);

        methodDef.declareAnnotation(Override.class);

        final Variable obj = methodDef.getScope().declareVariable(Object.class, "obj");
        final BytecodeBlock block = new BytecodeBlock();

        if (!valMarsh.isSimpleType())
            block.append(obj.set(methodDef.getThis().getField("valFactory", ObjectFactory.class)
                .invoke("create", Object.class)));

        block.append(valMarsh.unmarshallObject(classDef.getType(), methodDef.getScope().getVariable("row"), obj))
            .append(obj)
            .retObject();

        methodDef.getBody().append(block);
    }

    /**
     * Generates column size expression.
     *
     * @param obj Target object.
     * @param cols columns.
     * @param colIdx Column index.
     * @return Expression.
     */
    private BytecodeExpression getColumnValueSize(Variable obj, Variable cols, int colIdx) {
        return BytecodeExpressions.invokeStatic(MarshallerUtil.class, "getValueSize",
            int.class,
            Arrays.asList(Object.class, NativeType.class),
            obj,
            cols.invoke("column", Column.class, BytecodeExpressions.constantInt(colIdx))
                .invoke("type", NativeType.class)
        );
    }

    /**
     * Resolves current classloader.
     *
     * @return Classloader.
     */
    private static ClassLoader getClassLoader() {
        return Thread.currentThread().getContextClassLoader() == null ?
            ClassLoader.getSystemClassLoader() :
            Thread.currentThread().getContextClassLoader();
    }
}
