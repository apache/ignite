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

package org.apache.ignite.internal.binary;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryLowerCaseIdMapper;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.binary.BinaryOriginalNameMapper;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinarySimpleNameMapper;
import org.apache.ignite.binary.BinaryStraightIdMapper;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.test.GridBinaryTestClass1;
import org.apache.ignite.internal.binary.test.GridBinaryTestClass2;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Wildcards test.
 */
public class GridBinaryWildcardsSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String CLASS1_FULL_NAME = GridBinaryTestClass1.class.getName();

    /** */
    public static final String CLASS2_FULL_NAME = GridBinaryTestClass2.class.getName();

    /** */
    public static final String INNER_CLASS_FULL_NAME = GridBinaryTestClass1.class.getName() + "$InnerClass";

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesFullNameMapper() throws Exception {
        checkClassNames(new BinaryOriginalNameMapper(), new BinaryStraightIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesSimpleNameMapper() throws Exception {
        checkClassNames(new BinarySimpleNameMapper(), new BinaryLowerCaseIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesMixedMappers() throws Exception {
        checkClassNames(new BinaryOriginalNameMapper(), new BinaryLowerCaseIdMapper());
    }

    /**
     * @throws Exception If failed.
     * @param nameMapper Name mapper.
     * @param mapper ID mapper.
     */
    private void checkClassNames(BinaryNameMapper nameMapper, BinaryIdMapper mapper) throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(nameMapper, mapper, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, nameMapper, mapper)));
        assertTrue(typeIds.containsKey(typeId(CLASS2_FULL_NAME, nameMapper, mapper)));
        assertTrue(typeIds.containsKey(typeId(INNER_CLASS_FULL_NAME, nameMapper, mapper)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesCustomMappers() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(null, new BinaryIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 300;
                else if (clsName.endsWith("2"))
                    return 400;
                else if (clsName.endsWith("InnerClass"))
                    return 500;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<String, T2<BinaryNameMapper, BinaryIdMapper>> typeMappers = U.field(ctx, "cls2Mappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get(CLASS1_FULL_NAME).get2().typeId(CLASS1_FULL_NAME));
        assertEquals(400, typeMappers.get(CLASS2_FULL_NAME).get2().typeId(CLASS2_FULL_NAME));
        assertEquals(500, typeMappers.get(INNER_CLASS_FULL_NAME).get2().typeId(INNER_CLASS_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsSimpleNameIdMapper() throws Exception {
        checkTypeConfigurations(new BinarySimpleNameMapper(), new BinaryLowerCaseIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsFullNameIdMapper() throws Exception {
        checkTypeConfigurations(new BinaryOriginalNameMapper(), new BinaryStraightIdMapper());
    }

    /**
     *
     * @param nameMapper Name mapper.
     * @param idMapper ID mapper.
     * @throws IgniteCheckedException If failed.
     */
    private void checkTypeConfigurations(BinaryNameMapper nameMapper, BinaryIdMapper idMapper) throws IgniteCheckedException {
        BinaryMarshaller marsh = binaryMarshaller(nameMapper, idMapper, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, nameMapper, idMapper)));
        assertTrue(typeIds.containsKey(typeId(CLASS2_FULL_NAME, nameMapper, idMapper)));
        assertTrue(typeIds.containsKey(typeId(INNER_CLASS_FULL_NAME, nameMapper, idMapper)));
    }

    /**
     * @param typeName Type name.
     * @param nameMapper Name mapper.
     * @param mapper ID mapper.  @return Type ID.
     */
    private int typeId(String typeName, BinaryNameMapper nameMapper, BinaryIdMapper mapper) {
        if (mapper == null)
            mapper = BinaryContext.defaultIdMapper();

        if (nameMapper == null)
            nameMapper = BinaryContext.defaultNameMapper();

        return mapper.typeId(nameMapper.typeName(typeName));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithGlobalMapper() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinaryOriginalNameMapper(), new BinaryIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 300;
                else if (clsName.endsWith("2"))
                    return 400;
                else if (clsName.endsWith("InnerClass"))
                    return 500;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<String, T2<BinaryNameMapper, BinaryIdMapper>> typeMappers = U.field(ctx, "cls2Mappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get(CLASS1_FULL_NAME).get2().typeId(CLASS1_FULL_NAME));
        assertEquals(400, typeMappers.get(CLASS2_FULL_NAME).get2().typeId(CLASS2_FULL_NAME));
        assertEquals(500, typeMappers.get(INNER_CLASS_FULL_NAME).get2().typeId(INNER_CLASS_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithNonGlobalMapper() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinarySimpleNameMapper(), new BinaryIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 300;
                else if (clsName.endsWith("2"))
                    return 400;
                else if (clsName.endsWith("InnerClass"))
                    return 500;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<String, T2<BinaryNameMapper, BinaryIdMapper>> typeMappers = U.field(ctx, "cls2Mappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get(CLASS1_FULL_NAME).get2().typeId(CLASS1_FULL_NAME));
        assertEquals(400, typeMappers.get(CLASS2_FULL_NAME).get2().typeId(CLASS2_FULL_NAME));
        assertEquals(500, typeMappers.get(INNER_CLASS_FULL_NAME).get2().typeId(INNER_CLASS_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverrideIdMapperSimpleNameMapper() throws Exception {
        checkOverrideNameMapper(new BinarySimpleNameMapper(), new BinaryLowerCaseIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverrideIdMapperFullNameMapper() throws Exception {
        checkOverrideNameMapper(new BinaryOriginalNameMapper(), new BinaryStraightIdMapper());
    }

    /**
     *
     * @param nameMapper Name mapper.
     * @param mapper Mapper.
     * @throws IgniteCheckedException If failed.
     */
    private void checkOverrideIdMapper(BinaryNameMapper nameMapper, BinaryIdMapper mapper) throws IgniteCheckedException {
        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration();

        typeCfg.setTypeName(CLASS2_FULL_NAME);
        typeCfg.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 100;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryMarshaller marsh = binaryMarshaller(nameMapper, mapper, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            typeCfg));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, nameMapper, mapper)));
        assertTrue(typeIds.containsKey(typeId(INNER_CLASS_FULL_NAME, nameMapper, mapper)));
        assertTrue(typeIds.containsKey(100));

        Map<String, T2<BinaryNameMapper, BinaryIdMapper>> typeMappers = U.field(ctx, "cls2Mappers");

        assertEquals(100, typeMappers.get(CLASS2_FULL_NAME).get2().typeId(CLASS2_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverrideNameMapperSimpleNameMapper() throws Exception {
        checkOverrideNameMapper(new BinarySimpleNameMapper(), new BinaryLowerCaseIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverrideNameMapperFullNameMapper() throws Exception {
        checkOverrideNameMapper(new BinaryOriginalNameMapper(), new BinaryStraightIdMapper());
    }

    /**
     *
     * @param nameMapper Name mapper.
     * @param mapper Mapper.
     * @throws IgniteCheckedException If failed.
     */
    private void checkOverrideNameMapper(BinaryNameMapper nameMapper, BinaryIdMapper mapper) throws IgniteCheckedException {
        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration();

        typeCfg.setTypeName(CLASS2_FULL_NAME);
        typeCfg.setNameMapper(new BinaryNameMapper() {
            @Override public String typeName(String clsName) {
                return "type2";
            }

            @Override public String fieldName(String fieldName) {
                return "field2";
            }
        });

        BinaryMarshaller marsh = binaryMarshaller(nameMapper, mapper, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            typeCfg));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, nameMapper, mapper)));
        assertTrue(typeIds.containsKey(typeId(INNER_CLASS_FULL_NAME, nameMapper, mapper)));
        assertTrue(typeIds.containsKey("type2".hashCode()));

        Map<String, T2<BinaryNameMapper, BinaryIdMapper>> typeMappers = U.field(ctx, "cls2Mappers");

        assertEquals("type2", typeMappers.get(CLASS2_FULL_NAME).get1().typeName(CLASS2_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesJarFullNameMapper() throws Exception {
        checkClassNamesJar(new BinaryOriginalNameMapper(), new BinaryStraightIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesJarSimpleNameMapper() throws Exception {
        checkClassNamesJar(new BinarySimpleNameMapper(), new BinaryLowerCaseIdMapper());
    }

    /**
     *
     * @param nameMapper Name mapper.
     * @param idMapper Mapper.
     * @throws IgniteCheckedException If failed.
     */
    private void checkClassNamesJar(BinaryNameMapper nameMapper, BinaryIdMapper idMapper) throws IgniteCheckedException {
        BinaryMarshaller marsh = binaryMarshaller(nameMapper, idMapper, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, nameMapper, idMapper)));
        assertTrue(typeIds.containsKey(typeId(CLASS2_FULL_NAME, nameMapper, idMapper)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesWithCustomMapperJar() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinaryOriginalNameMapper(), new BinaryIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 300;
                else if (clsName.endsWith("2"))
                    return 400;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<String, T2<BinaryNameMapper, BinaryIdMapper>> typeMappers = U.field(ctx, "cls2Mappers");

        assertEquals(3, typeMappers.size());

        assertTrue(typeMappers.get(CLASS1_FULL_NAME).get1() instanceof BinaryOriginalNameMapper);
        assertEquals(300, typeMappers.get(CLASS1_FULL_NAME).get2().typeId(CLASS1_FULL_NAME));

        assertTrue(typeMappers.get(CLASS2_FULL_NAME).get1() instanceof BinaryOriginalNameMapper);
        assertEquals(400, typeMappers.get(CLASS2_FULL_NAME).get2().typeId(CLASS2_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsJarSimpleNameMapper() throws Exception {
        checkTypeConfigurationJar(new BinarySimpleNameMapper(), new BinaryLowerCaseIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsJarFullNameMapper() throws Exception {
        checkTypeConfigurationJar(new BinaryOriginalNameMapper(), new BinaryStraightIdMapper());
    }

    /**
     *
     * @param nameMapper
     * @param idMapper Mapper.
     * @throws IgniteCheckedException If failed.
     */
    private void checkTypeConfigurationJar(BinaryNameMapper nameMapper, BinaryIdMapper idMapper)
        throws IgniteCheckedException {
        BinaryMarshaller marsh = binaryMarshaller(nameMapper, idMapper, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, nameMapper, idMapper)));
        assertTrue(typeIds.containsKey(typeId(CLASS2_FULL_NAME, nameMapper, idMapper)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithGlobalMapperJar() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinaryOriginalNameMapper(), new BinaryIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 300;
                else if (clsName.endsWith("2"))
                    return 400;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<String, T2<BinaryNameMapper, BinaryIdMapper>> typeMappers = U.field(ctx, "cls2Mappers");

        assertEquals(3, typeMappers.size());

        assertTrue(typeMappers.get(CLASS1_FULL_NAME).get1() instanceof BinaryOriginalNameMapper);
        assertEquals(300, typeMappers.get(CLASS1_FULL_NAME).get2().typeId(CLASS1_FULL_NAME));

        assertTrue(typeMappers.get(CLASS2_FULL_NAME).get1() instanceof BinaryOriginalNameMapper);
        assertEquals(400, typeMappers.get(CLASS2_FULL_NAME).get2().typeId(CLASS2_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithNonGlobalMapperJar() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinaryOriginalNameMapper(), new BinaryIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 300;
                else if (clsName.endsWith("2"))
                    return 400;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<String, T2<BinaryNameMapper, BinaryIdMapper>> typeMappers = U.field(ctx, "cls2Mappers");

        assertEquals(3, typeMappers.size());

        assertTrue(typeMappers.get(CLASS1_FULL_NAME).get1() instanceof BinaryOriginalNameMapper);
        assertEquals(300, typeMappers.get(CLASS1_FULL_NAME).get2().typeId(CLASS1_FULL_NAME));

        assertTrue(typeMappers.get(CLASS2_FULL_NAME).get1() instanceof BinaryOriginalNameMapper);
        assertEquals(400, typeMappers.get(CLASS2_FULL_NAME).get2().typeId(CLASS2_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverrideJarSimpleNameMapper() throws Exception {
        checkOverrideJar(new BinarySimpleNameMapper(), new BinaryLowerCaseIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverrideJarFullNameMapper() throws Exception {
        checkOverrideJar(new BinaryOriginalNameMapper(), new BinaryStraightIdMapper());
    }

    /**
     *
     * @param nameMapper Name mapper.
     * @param idMapper Mapper.
     * @throws IgniteCheckedException If failed.
     */
    private void checkOverrideJar(BinaryNameMapper nameMapper, BinaryIdMapper idMapper) throws IgniteCheckedException {
        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration(
            "org.apache.ignite.internal.binary.test.GridBinaryTestClass2");

        typeCfg.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 100;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryMarshaller marsh = binaryMarshaller(nameMapper, idMapper, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            typeCfg));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, nameMapper, idMapper)));

        Map<String, T2<BinaryNameMapper, BinaryIdMapper>> typeMappers = U.field(ctx, "cls2Mappers");

        assertEquals(3, typeMappers.size());

        assertEquals(nameMapper, typeMappers.get(CLASS2_FULL_NAME).get1());
        assertEquals(100, typeMappers.get(CLASS2_FULL_NAME).get2().typeId(CLASS2_FULL_NAME));
    }

    /**
     * @param marsh Marshaller.
     * @return Binary context.
     */
    protected BinaryContext binaryContext(BinaryMarshaller marsh) {
        GridBinaryMarshaller impl = U.field(marsh, "impl");

        return impl.context();
    }

    /**
     *
     */
    protected BinaryMarshaller binaryMarshaller()
        throws IgniteCheckedException {
        return binaryMarshaller(null, null, null);
    }

    /**
     *
     */
    protected BinaryMarshaller binaryMarshaller(Collection<BinaryTypeConfiguration> cfgs)
        throws IgniteCheckedException {
        return binaryMarshaller(null, null, cfgs);
    }

    /**
     *
     */
    protected BinaryMarshaller binaryMarshaller(BinaryNameMapper nameMapper, BinaryIdMapper mapper,
        Collection<BinaryTypeConfiguration> cfgs)
        throws IgniteCheckedException {
        return binaryMarshaller(nameMapper, mapper, null, cfgs);
    }

    /**
     *
     */
    protected BinaryMarshaller binaryMarshaller(BinarySerializer serializer, Collection<BinaryTypeConfiguration> cfgs)
        throws IgniteCheckedException {
        return binaryMarshaller(null, null, serializer, cfgs);
    }

    /**
     *
     */
    protected BinaryMarshaller binaryMarshaller(
        BinaryNameMapper nameMapper,
        BinaryIdMapper mapper,
        BinarySerializer serializer,
        Collection<BinaryTypeConfiguration> cfgs
    ) throws IgniteCheckedException {
        IgniteConfiguration iCfg = new IgniteConfiguration();

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setNameMapper(nameMapper);
        bCfg.setIdMapper(mapper);
        bCfg.setSerializer(serializer);

        bCfg.setTypeConfigurations(cfgs);

        iCfg.setBinaryConfiguration(bCfg);

        BinaryContext ctx = new BinaryContext(BinaryNoopMetadataHandler.instance(), iCfg, new NullLogger());

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new MarshallerContextTestImpl(null));

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, iCfg);

        return marsh;
    }
}
