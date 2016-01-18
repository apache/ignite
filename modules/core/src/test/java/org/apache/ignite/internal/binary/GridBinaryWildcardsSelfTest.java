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
import org.apache.ignite.binary.BinaryFullNameIdMapper;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinarySimpleNameIdMapper;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.test.GridBinaryTestClass1;
import org.apache.ignite.internal.binary.test.GridBinaryTestClass2;
import org.apache.ignite.internal.util.IgniteUtils;
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
        checkClassNames(new BinaryFullNameIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesSimpleNameMapper() throws Exception {
        checkClassNames(new BinarySimpleNameIdMapper());
    }

    /**
     * @throws Exception If failed.
     * @param mapper
     */
    private void checkClassNames(BinaryIdMapper mapper) throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(mapper, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, mapper)));
        assertTrue(typeIds.containsKey(typeId(CLASS2_FULL_NAME, mapper)));
        assertTrue(typeIds.containsKey(typeId(INNER_CLASS_FULL_NAME, mapper)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesWithMapper() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinaryIdMapper() {
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

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get(CLASS1_FULL_NAME).typeId(CLASS1_FULL_NAME));
        assertEquals(400, typeMappers.get(CLASS2_FULL_NAME).typeId(CLASS2_FULL_NAME));
        assertEquals(500, typeMappers.get(INNER_CLASS_FULL_NAME).typeId(INNER_CLASS_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsSimpleNameIdMapper() throws Exception {
        checkTypeConfigurations(new BinarySimpleNameIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsFullNameIdMapper() throws Exception {
        checkTypeConfigurations(new BinaryFullNameIdMapper());
    }

    /**
     * @param idMapper ID mapper.
     * @throws IgniteCheckedException If failed.
     */
    private void checkTypeConfigurations(BinaryIdMapper idMapper) throws IgniteCheckedException {
        BinaryMarshaller marsh = binaryMarshaller(idMapper, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, idMapper)));
        assertTrue(typeIds.containsKey(typeId(CLASS2_FULL_NAME, idMapper)));
        assertTrue(typeIds.containsKey(typeId(INNER_CLASS_FULL_NAME, idMapper)));
    }

    /**
     * @param typeName Type name.
     * @param mapper ID mapper.
     * @return Type ID.
     */
    private int typeId(String typeName, BinaryIdMapper mapper) {
        if (mapper != null)
            return mapper.typeId(typeName);

        return BinaryContext.defaultIdMapper().typeId(typeName);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithGlobalMapper() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinaryIdMapper() {
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

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get(CLASS1_FULL_NAME).typeId(CLASS1_FULL_NAME));
        assertEquals(400, typeMappers.get(CLASS2_FULL_NAME).typeId(CLASS2_FULL_NAME));
        assertEquals(500, typeMappers.get(INNER_CLASS_FULL_NAME).typeId(INNER_CLASS_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithNonGlobalMapper() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinaryIdMapper() {
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

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get(CLASS1_FULL_NAME).typeId(CLASS1_FULL_NAME));
        assertEquals(400, typeMappers.get(CLASS2_FULL_NAME).typeId(CLASS2_FULL_NAME));
        assertEquals(500, typeMappers.get(INNER_CLASS_FULL_NAME).typeId(INNER_CLASS_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverrideSimpleNameMapper() throws Exception {
        checkOverride(new BinarySimpleNameIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverrideFullNameMapper() throws Exception {
        checkOverride(new BinaryFullNameIdMapper());
    }

    /**
     * @param mapper Mapper.
     * @throws IgniteCheckedException If failed.
     */
    private void checkOverride(BinaryIdMapper mapper) throws IgniteCheckedException {
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

        BinaryMarshaller marsh = binaryMarshaller(mapper, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            typeCfg));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, mapper)));
        assertTrue(typeIds.containsKey(typeId(INNER_CLASS_FULL_NAME, mapper)));
        assertTrue(typeIds.containsKey(100));

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(100, typeMappers.get(CLASS2_FULL_NAME).typeId(CLASS2_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesJarFullNameMapper() throws Exception {
        checkClassNamesJar(new BinaryFullNameIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesJarSimpleNameMapper() throws Exception {
        checkClassNamesJar(new BinaryFullNameIdMapper());
    }

    /**
     * @param mapper Mapper.
     * @throws IgniteCheckedException If failed.
     */
    private void checkClassNamesJar(BinaryIdMapper mapper) throws IgniteCheckedException {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, mapper)));
        assertTrue(typeIds.containsKey(typeId(CLASS2_FULL_NAME, mapper)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesWithCustomMapperJar() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinaryIdMapper() {
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

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get(CLASS1_FULL_NAME).typeId(CLASS1_FULL_NAME));
        assertEquals(400, typeMappers.get(CLASS2_FULL_NAME).typeId(CLASS2_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsJarSimpleNameMapper() throws Exception {
        checkTypeConfigurationJar(new BinarySimpleNameIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsJarFullNameMapper() throws Exception {
        checkTypeConfigurationJar(new BinaryFullNameIdMapper());
    }

    /**
     * @param mapper Mapper.
     * @throws IgniteCheckedException If failed.
     */
    private void checkTypeConfigurationJar(BinaryIdMapper mapper) throws IgniteCheckedException {
        BinaryMarshaller marsh = binaryMarshaller(mapper, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, mapper)));
        assertTrue(typeIds.containsKey(typeId(CLASS2_FULL_NAME, mapper)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithGlobalMapperJar() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinaryIdMapper() {
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

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get(CLASS1_FULL_NAME).typeId(CLASS1_FULL_NAME));
        assertEquals(400, typeMappers.get(CLASS2_FULL_NAME).typeId(CLASS2_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithNonGlobalMapperJar() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinaryIdMapper() {
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

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get(CLASS1_FULL_NAME).typeId(CLASS1_FULL_NAME));
        assertEquals(400, typeMappers.get(CLASS2_FULL_NAME).typeId(CLASS2_FULL_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverrideJarSimpleNameMapper() throws Exception {
        checkOverrideJar(new BinarySimpleNameIdMapper());
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverrideJarFullNameMapper() throws Exception {
        checkOverrideJar(new BinaryFullNameIdMapper());
    }

    /**
     * @param mapper Mapper.
     * @throws IgniteCheckedException If failed.
     */
    private void checkOverrideJar(BinaryIdMapper mapper) throws IgniteCheckedException {
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

        BinaryMarshaller marsh = binaryMarshaller(mapper, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.binary.test.*"),
            typeCfg));

        BinaryContext ctx = binaryContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey(typeId(CLASS1_FULL_NAME, mapper)));

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(100, typeMappers.get(CLASS2_FULL_NAME).typeId(CLASS2_FULL_NAME));
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
    protected BinaryMarshaller binaryMarshaller(BinaryIdMapper mapper, Collection<BinaryTypeConfiguration> cfgs)
        throws IgniteCheckedException {
        return binaryMarshaller(mapper, null, cfgs);
    }

    /**
     *
     */
    protected BinaryMarshaller binaryMarshaller(BinarySerializer serializer, Collection<BinaryTypeConfiguration> cfgs)
        throws IgniteCheckedException {
        return binaryMarshaller(null, serializer, cfgs);
    }

    protected BinaryMarshaller binaryMarshaller(
        BinaryIdMapper mapper,
        BinarySerializer serializer,
        Collection<BinaryTypeConfiguration> cfgs
    ) throws IgniteCheckedException {
        IgniteConfiguration iCfg = new IgniteConfiguration();

        BinaryConfiguration bCfg = new BinaryConfiguration();

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
