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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryIdentityResolver;
import org.apache.ignite.binary.BinaryReflectiveSerializer;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.cache.binary.BinaryMetadataKey;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.datastructures.CollocatedQueueItemKey;
import org.apache.ignite.internal.processors.datastructures.CollocatedSetItemKey;
import org.apache.ignite.internal.processors.igfs.IgfsBlockKey;
import org.apache.ignite.internal.processors.igfs.IgfsDirectoryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsFileAffinityRange;
import org.apache.ignite.internal.processors.igfs.IgfsFileInfo;
import org.apache.ignite.internal.processors.igfs.IgfsFileMap;
import org.apache.ignite.internal.processors.igfs.IgfsListingEntry;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientAffinityCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientDeleteCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientExistsCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientInfoCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientListFilesCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientListPathsCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientMkdirsCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientRenameCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientSetTimesCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientSizeCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientSummaryCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientUpdateCallable;
import org.apache.ignite.internal.processors.igfs.client.meta.IgfsClientMetaIdsForPathCallable;
import org.apache.ignite.internal.processors.igfs.client.meta.IgfsClientMetaInfoForPathCallable;
import org.apache.ignite.internal.processors.igfs.client.meta.IgfsClientMetaUnlockCallable;
import org.apache.ignite.internal.processors.igfs.data.IgfsDataPutProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaDirectoryCreateProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaDirectoryListingAddProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaDirectoryListingRemoveProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaDirectoryListingRenameProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaDirectoryListingReplaceProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaFileCreateProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaFileLockProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaFileRangeDeleteProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaFileRangeUpdateProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaFileReserveSpaceProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaFileUnlockProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaUpdatePropertiesProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaUpdateTimesProcessor;
import org.apache.ignite.internal.processors.platform.PlatformJavaObjectFactoryProxy;
import org.apache.ignite.internal.processors.platform.websession.PlatformDotNetSessionData;
import org.apache.ignite.internal.processors.platform.websession.PlatformDotNetSessionLockResult;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Binary context.
 */
public class BinaryContext {
    /** System loader.*/
    private static final ClassLoader sysLdr = U.gridClassLoader();

    /** */
    private static final BinaryInternalMapper DFLT_MAPPER =
        new BinaryInternalMapper(new BinaryBasicNameMapper(false), new BinaryBasicIdMapper(true), false);

    /** */
    static final BinaryInternalMapper SIMPLE_NAME_LOWER_CASE_MAPPER =
        new BinaryInternalMapper(new BinaryBasicNameMapper(true), new BinaryBasicIdMapper(true), false);

    /** Set of system classes that should be marshalled with BinaryMarshaller. */
    private static final Set<String> BINARYLIZABLE_SYS_CLSS;

    /* Binarylizable system classes set initialization. */
    static {
        Set<String> sysClss = new HashSet<>();

        // IGFS classes.
        sysClss.add(IgfsPath.class.getName());

        sysClss.add(IgfsBlockKey.class.getName());
        sysClss.add(IgfsDirectoryInfo.class.getName());
        sysClss.add(IgfsFileAffinityRange.class.getName());
        sysClss.add(IgfsFileInfo.class.getName());
        sysClss.add(IgfsFileMap.class.getName());
        sysClss.add(IgfsListingEntry.class.getName());

        sysClss.add(IgfsDataPutProcessor.class.getName());

        sysClss.add(IgfsMetaDirectoryCreateProcessor.class.getName());
        sysClss.add(IgfsMetaDirectoryListingAddProcessor.class.getName());
        sysClss.add(IgfsMetaDirectoryListingRemoveProcessor.class.getName());
        sysClss.add(IgfsMetaDirectoryListingRenameProcessor.class.getName());
        sysClss.add(IgfsMetaDirectoryListingReplaceProcessor.class.getName());
        sysClss.add(IgfsMetaFileCreateProcessor.class.getName());
        sysClss.add(IgfsMetaFileLockProcessor.class.getName());
        sysClss.add(IgfsMetaFileRangeDeleteProcessor.class.getName());
        sysClss.add(IgfsMetaFileRangeUpdateProcessor.class.getName());
        sysClss.add(IgfsMetaFileReserveSpaceProcessor.class.getName());
        sysClss.add(IgfsMetaFileUnlockProcessor.class.getName());
        sysClss.add(IgfsMetaUpdatePropertiesProcessor.class.getName());
        sysClss.add(IgfsMetaUpdateTimesProcessor.class.getName());

        sysClss.add(IgfsClientMetaIdsForPathCallable.class.getName());
        sysClss.add(IgfsClientMetaInfoForPathCallable.class.getName());
        sysClss.add(IgfsClientMetaUnlockCallable.class.getName());

        sysClss.add(IgfsClientAffinityCallable.class.getName());
        sysClss.add(IgfsClientDeleteCallable.class.getName());
        sysClss.add(IgfsClientExistsCallable.class.getName());
        sysClss.add(IgfsClientInfoCallable.class.getName());
        sysClss.add(IgfsClientListFilesCallable.class.getName());
        sysClss.add(IgfsClientListPathsCallable.class.getName());
        sysClss.add(IgfsClientMkdirsCallable.class.getName());
        sysClss.add(IgfsClientRenameCallable.class.getName());
        sysClss.add(IgfsClientSetTimesCallable.class.getName());
        sysClss.add(IgfsClientSizeCallable.class.getName());
        sysClss.add(IgfsClientSummaryCallable.class.getName());
        sysClss.add(IgfsClientUpdateCallable.class.getName());

        // Closure processor classes.
        sysClss.add(GridClosureProcessor.C1V2.class.getName());
        sysClss.add(GridClosureProcessor.C1MLAV2.class.getName());
        sysClss.add(GridClosureProcessor.C2V2.class.getName());
        sysClss.add(GridClosureProcessor.C2MLAV2.class.getName());
        sysClss.add(GridClosureProcessor.C4V2.class.getName());
        sysClss.add(GridClosureProcessor.C4MLAV2.class.getName());

        if (BinaryUtils.wrapTrees()) {
            sysClss.add(TreeMap.class.getName());
            sysClss.add(TreeSet.class.getName());
        }

        BINARYLIZABLE_SYS_CLSS = Collections.unmodifiableSet(sysClss);
    }

    /** */
    private final ConcurrentMap<Class<?>, BinaryClassDescriptor> descByCls = new ConcurrentHashMap8<>();

    /** */
    private final Map<Integer, BinaryClassDescriptor> predefinedTypes = new HashMap<>();

    /** */
    private final Map<String, Integer> predefinedTypeNames = new HashMap<>();

    /** */
    private final Map<Class<? extends Collection>, Byte> colTypes = new HashMap<>();

    /** */
    private final Map<Class<? extends Map>, Byte> mapTypes = new HashMap<>();

    /** Maps typeId to mappers. */
    private final ConcurrentMap<Integer, BinaryInternalMapper> typeId2Mapper = new ConcurrentHashMap8<>(0);

    /** Affinity key field names. */
    private final ConcurrentMap<Integer, String> affKeyFieldNames = new ConcurrentHashMap8<>(0);

    /** Maps className to mapper */
    private final ConcurrentMap<String, BinaryInternalMapper> cls2Mappers = new ConcurrentHashMap8<>(0);

    /** Affinity key field names. */
    private final ConcurrentMap<Integer, BinaryIdentityResolver> identities = new ConcurrentHashMap8<>(0);

    /** */
    private BinaryMetadataHandler metaHnd;

    /** Actual marshaller. */
    private BinaryMarshaller marsh;

    /** */
    private MarshallerContext marshCtx;

    /** */
    private IgniteConfiguration igniteCfg;

    /** Logger. */
    private IgniteLogger log;

    /** */
    private final OptimizedMarshaller optmMarsh = new OptimizedMarshaller(false);

    /** Compact footer flag. */
    private boolean compactFooter;

    /** Object schemas. */
    private volatile Map<Integer, BinarySchemaRegistry> schemas;

    /**
     * @param metaHnd Meta data handler.
     * @param igniteCfg Ignite configuration.
     * @param log Logger.
     */
    public BinaryContext(BinaryMetadataHandler metaHnd, IgniteConfiguration igniteCfg, IgniteLogger log) {
        assert metaHnd != null;
        assert igniteCfg != null;

        MarshallerUtils.setNodeName(optmMarsh, igniteCfg.getGridName());

        this.metaHnd = metaHnd;
        this.igniteCfg = igniteCfg;
        this.log = log;

        colTypes.put(ArrayList.class, GridBinaryMarshaller.ARR_LIST);
        colTypes.put(LinkedList.class, GridBinaryMarshaller.LINKED_LIST);
        colTypes.put(HashSet.class, GridBinaryMarshaller.HASH_SET);
        colTypes.put(LinkedHashSet.class, GridBinaryMarshaller.LINKED_HASH_SET);

        mapTypes.put(HashMap.class, GridBinaryMarshaller.HASH_MAP);
        mapTypes.put(LinkedHashMap.class, GridBinaryMarshaller.LINKED_HASH_MAP);

        // IDs range from [0..200] is used by Java SDK API and GridGain legacy API

        registerPredefinedType(Byte.class, GridBinaryMarshaller.BYTE);
        registerPredefinedType(Boolean.class, GridBinaryMarshaller.BOOLEAN);
        registerPredefinedType(Short.class, GridBinaryMarshaller.SHORT);
        registerPredefinedType(Character.class, GridBinaryMarshaller.CHAR);
        registerPredefinedType(Integer.class, GridBinaryMarshaller.INT);
        registerPredefinedType(Long.class, GridBinaryMarshaller.LONG);
        registerPredefinedType(Float.class, GridBinaryMarshaller.FLOAT);
        registerPredefinedType(Double.class, GridBinaryMarshaller.DOUBLE);
        registerPredefinedType(String.class, GridBinaryMarshaller.STRING);
        registerPredefinedType(BigDecimal.class, GridBinaryMarshaller.DECIMAL);
        registerPredefinedType(Date.class, GridBinaryMarshaller.DATE);
        registerPredefinedType(Timestamp.class, GridBinaryMarshaller.TIMESTAMP);
        registerPredefinedType(UUID.class, GridBinaryMarshaller.UUID);

        registerPredefinedType(byte[].class, GridBinaryMarshaller.BYTE_ARR);
        registerPredefinedType(short[].class, GridBinaryMarshaller.SHORT_ARR);
        registerPredefinedType(int[].class, GridBinaryMarshaller.INT_ARR);
        registerPredefinedType(long[].class, GridBinaryMarshaller.LONG_ARR);
        registerPredefinedType(float[].class, GridBinaryMarshaller.FLOAT_ARR);
        registerPredefinedType(double[].class, GridBinaryMarshaller.DOUBLE_ARR);
        registerPredefinedType(char[].class, GridBinaryMarshaller.CHAR_ARR);
        registerPredefinedType(boolean[].class, GridBinaryMarshaller.BOOLEAN_ARR);
        registerPredefinedType(BigDecimal[].class, GridBinaryMarshaller.DECIMAL_ARR);
        registerPredefinedType(String[].class, GridBinaryMarshaller.STRING_ARR);
        registerPredefinedType(UUID[].class, GridBinaryMarshaller.UUID_ARR);
        registerPredefinedType(Date[].class, GridBinaryMarshaller.DATE_ARR);
        registerPredefinedType(Timestamp[].class, GridBinaryMarshaller.TIMESTAMP_ARR);
        registerPredefinedType(Object[].class, GridBinaryMarshaller.OBJ_ARR);

        // Special collections.
        registerPredefinedType(ArrayList.class, 0);
        registerPredefinedType(LinkedList.class, 0);
        registerPredefinedType(HashSet.class, 0);
        registerPredefinedType(LinkedHashSet.class, 0);
        registerPredefinedType(HashMap.class, 0);
        registerPredefinedType(LinkedHashMap.class, 0);

        // Classes with overriden default serialization flag.
        registerPredefinedType(AffinityKey.class, 0, affinityFieldName(AffinityKey.class), false);

        registerPredefinedType(GridMapEntry.class, 60);
        registerPredefinedType(IgniteBiTuple.class, 61);
        registerPredefinedType(T2.class, 62);

        registerPredefinedType(PlatformJavaObjectFactoryProxy.class,
            GridBinaryMarshaller.PLATFORM_JAVA_OBJECT_FACTORY_PROXY);

        registerPredefinedType(BinaryObjectImpl.class, 0);
        registerPredefinedType(BinaryObjectOffheapImpl.class, 0);
        registerPredefinedType(BinaryMetadataKey.class, 0);
        registerPredefinedType(BinaryMetadata.class, 0);
        registerPredefinedType(BinaryEnumObjectImpl.class, 0);

        registerPredefinedType(PlatformDotNetSessionData.class, 0);
        registerPredefinedType(PlatformDotNetSessionLockResult.class, 0);

        // IDs range [200..1000] is used by Ignite internal APIs.
    }

    /**
     * @return Logger.
     */
    public IgniteLogger log() {
        return log;
    }

    /**
     * @return Marshaller.
     */
    public BinaryMarshaller marshaller() {
        return marsh;
    }

    /**
     * Check whether class must be deserialized anyway.
     *
     * @param cls Class.
     * @return {@code True} if must be deserialized.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    public boolean mustDeserialize(Class cls) {
        BinaryClassDescriptor desc = descByCls.get(cls);

        if (desc == null) {
            if (BinaryUtils.wrapTrees() && (cls == TreeMap.class || cls == TreeSet.class))
                return false;

            return marshCtx.isSystemType(cls.getName()) || serializerForClass(cls) == null ||
                GridQueryProcessor.isGeometryClass(cls);
        }
        else
            return desc.useOptimizedMarshaller();
    }

    /**
     * @return Ignite configuration.
     */
    public IgniteConfiguration configuration() {
        return igniteCfg;
    }

    /**
     * @param marsh Binary marshaller.
     * @param cfg Configuration.
     * @throws BinaryObjectException In case of error.
     */
    public void configure(BinaryMarshaller marsh, IgniteConfiguration cfg) throws BinaryObjectException {
        if (marsh == null)
            return;

        this.marsh = marsh;

        marshCtx = marsh.getContext();

        BinaryConfiguration binaryCfg = cfg.getBinaryConfiguration();

        if (binaryCfg == null)
            binaryCfg = new BinaryConfiguration();

        assert marshCtx != null;

        optmMarsh.setContext(marshCtx);

        configure(
            binaryCfg.getNameMapper(),
            binaryCfg.getIdMapper(),
            binaryCfg.getSerializer(),
            binaryCfg.getTypeConfigurations()
        );

        compactFooter = binaryCfg.isCompactFooter();
    }

    /**
     * @param globalIdMapper ID mapper.
     * @param globalSerializer Serializer.
     * @param typeCfgs Type configurations.
     * @throws BinaryObjectException In case of error.
     */
    private void configure(
        BinaryNameMapper globalNameMapper,
        BinaryIdMapper globalIdMapper,
        BinarySerializer globalSerializer,
        Collection<BinaryTypeConfiguration> typeCfgs
    ) throws BinaryObjectException {
        TypeDescriptors descs = new TypeDescriptors();

        Map<String, String> affFields = new HashMap<>();

        if (!F.isEmpty(igniteCfg.getCacheKeyConfiguration())) {
            for (CacheKeyConfiguration keyCfg : igniteCfg.getCacheKeyConfiguration())
                affFields.put(keyCfg.getTypeName(), keyCfg.getAffinityKeyFieldName());
        }

        if (typeCfgs != null) {
            for (BinaryTypeConfiguration typeCfg : typeCfgs) {
                String clsName = typeCfg.getTypeName();

                if (clsName == null)
                    throw new BinaryObjectException("Class name is required for binary type configuration.");

                // Resolve mapper.
                BinaryIdMapper idMapper = U.firstNotNull(typeCfg.getIdMapper(), globalIdMapper);
                BinaryNameMapper nameMapper = U.firstNotNull(typeCfg.getNameMapper(), globalNameMapper);
                BinarySerializer serializer = U.firstNotNull(typeCfg.getSerializer(), globalSerializer);
                BinaryIdentityResolver identity = typeCfg.getIdentityResolver();

                BinaryInternalMapper mapper = resolveMapper(nameMapper, idMapper);

                if (clsName.endsWith(".*")) {
                    String pkgName = clsName.substring(0, clsName.length() - 2);

                    for (String clsName0 : classesInPackage(pkgName))
                        descs.add(clsName0, mapper, serializer, identity, affFields.get(clsName0),
                            typeCfg.isEnum(), true);
                }
                else
                    descs.add(clsName, mapper, serializer, identity, affFields.get(clsName),
                        typeCfg.isEnum(), false);
            }
        }

        for (TypeDescriptor desc : descs.descriptors())
            registerUserType(desc.clsName, desc.mapper, desc.serializer, desc.identity, desc.affKeyFieldName,
                desc.isEnum);

        BinaryInternalMapper globalMapper = resolveMapper(globalNameMapper, globalIdMapper);

        // Put affinity field names for unconfigured types.
        for (Map.Entry<String, String> entry : affFields.entrySet()) {
            String typeName = entry.getKey();

            int typeId = globalMapper.typeId(typeName);

            affKeyFieldNames.putIfAbsent(typeId, entry.getValue());
        }

        addSystemClassAffinityKey(CollocatedSetItemKey.class);
        addSystemClassAffinityKey(CollocatedQueueItemKey.class);
    }

    /**
     * @param nameMapper Name mapper.
     * @param idMapper ID mapper.
     * @return Mapper.
     */
    private static BinaryInternalMapper resolveMapper(BinaryNameMapper nameMapper, BinaryIdMapper idMapper) {
        if ((nameMapper == null || (DFLT_MAPPER.nameMapper().equals(nameMapper)))
            && (idMapper == null || DFLT_MAPPER.idMapper().equals(idMapper)))
            return DFLT_MAPPER;

        if (nameMapper != null && nameMapper instanceof BinaryBasicNameMapper
            && ((BinaryBasicNameMapper)nameMapper).isSimpleName()
            && idMapper != null && idMapper instanceof BinaryBasicIdMapper
            && ((BinaryBasicIdMapper)idMapper).isLowerCase())
            return SIMPLE_NAME_LOWER_CASE_MAPPER;

        if (nameMapper == null)
            nameMapper = DFLT_MAPPER.nameMapper();

        if (idMapper == null)
            idMapper = DFLT_MAPPER.idMapper();

        return new BinaryInternalMapper(nameMapper, idMapper, true);
    }

    /**
     * @return Internal mapper used as default.
     */
    public static BinaryInternalMapper defaultMapper() {
        return DFLT_MAPPER;
    }

    /**
     * @return ID mapper used as default.
     */
    public static BinaryIdMapper defaultIdMapper() {
        return DFLT_MAPPER.idMapper();
    }

    /**
     * @return Name mapper used as default.
     */
    public static BinaryNameMapper defaultNameMapper() {
        return DFLT_MAPPER.nameMapper();
    }

    /**
     * @param cls Class.
     */
    private void addSystemClassAffinityKey(Class<?> cls) {
        String fieldName = affinityFieldName(cls);

        assert fieldName != null : cls;

        affKeyFieldNames.putIfAbsent(cls.getName().hashCode(), affinityFieldName(cls));
    }

    /**
     * @param pkgName Package name.
     * @return Class names.
     */
    @SuppressWarnings("ConstantConditions")
    private static Iterable<String> classesInPackage(String pkgName) {
        assert pkgName != null;

        Collection<String> clsNames = new ArrayList<>();

        ClassLoader ldr = U.gridClassLoader();

        if (ldr instanceof URLClassLoader) {
            String pkgPath = pkgName.replaceAll("\\.", "/");

            URL[] urls = ((URLClassLoader)ldr).getURLs();

            for (URL url : urls) {
                String proto = url.getProtocol().toLowerCase();

                if ("file".equals(proto)) {
                    try {
                        File cpElement = new File(url.toURI());

                        if (cpElement.isDirectory()) {
                            File pkgDir = new File(cpElement, pkgPath);

                            if (pkgDir.isDirectory()) {
                                for (File file : pkgDir.listFiles()) {
                                    String fileName = file.getName();

                                    if (file.isFile() && fileName.toLowerCase().endsWith(".class"))
                                        clsNames.add(pkgName + '.' + fileName.substring(0, fileName.length() - 6));
                                }
                            }
                        }
                        else if (cpElement.isFile()) {
                            try {
                                JarFile jar = new JarFile(cpElement);

                                Enumeration<JarEntry> entries = jar.entries();

                                while (entries.hasMoreElements()) {
                                    String entry = entries.nextElement().getName();

                                    if (entry.startsWith(pkgPath) && entry.endsWith(".class")) {
                                        String clsName = entry.substring(pkgPath.length() + 1, entry.length() - 6);

                                        if (!clsName.contains("/") && !clsName.contains("\\"))
                                            clsNames.add(pkgName + '.' + clsName);
                                    }
                                }
                            }
                            catch (IOException ignored) {
                                // No-op.
                            }
                        }
                    }
                    catch (URISyntaxException ignored) {
                        // No-op.
                    }
                }
            }
        }

        return clsNames;
    }

    /**
     * @param cls Class.
     * @return Class descriptor.
     * @throws BinaryObjectException In case of error.
     */
    public BinaryClassDescriptor descriptorForClass(Class<?> cls, boolean deserialize)
        throws BinaryObjectException {
        assert cls != null;

        BinaryClassDescriptor desc = descByCls.get(cls);

        if (desc == null)
            desc = registerClassDescriptor(cls, deserialize);
        else if (!desc.registered()) {
            if (!desc.userType()) {
                BinaryClassDescriptor desc0 = new BinaryClassDescriptor(
                    this,
                    desc.describedClass(),
                    false,
                    desc.typeId(),
                    desc.typeName(),
                    desc.affFieldKeyName(),
                    desc.mapper(),
                    desc.initialSerializer(),
                    false,
                    true
                );

                if (descByCls.replace(cls, desc, desc0)) {
                    Collection<BinarySchema> schemas =
                        desc0.schema() != null ? Collections.singleton(desc.schema()) : null;

                    BinaryMetadata meta = new BinaryMetadata(desc0.typeId(),
                        desc0.typeName(),
                        desc0.fieldsMeta(),
                        desc0.affFieldKeyName(),
                        schemas, desc0.isEnum());

                    metaHnd.addMeta(desc0.typeId(), meta.wrap(this));

                    return desc0;
                }
            }
            else
                desc = registerUserClassDescriptor(desc);
        }

        return desc;
    }

    /**
     * @param userType User type or not.
     * @param typeId Type ID.
     * @param ldr Class loader.
     * @return Class descriptor.
     */
    public BinaryClassDescriptor descriptorForTypeId(
        boolean userType,
        int typeId,
        ClassLoader ldr,
        boolean deserialize
    ) {
        assert typeId != GridBinaryMarshaller.UNREGISTERED_TYPE_ID;

        //TODO: As a workaround for IGNITE-1358 we always check the predefined map before without checking 'userType'
        BinaryClassDescriptor desc = predefinedTypes.get(typeId);

        if (desc != null)
            return desc;

        if (ldr == null)
            ldr = sysLdr;

        Class cls;

        try {
            cls = marshCtx.getClass(typeId, ldr);

            desc = descByCls.get(cls);
        }
        catch (ClassNotFoundException e) {
            // Class might have been loaded by default class loader.
            if (userType && !ldr.equals(sysLdr) && (desc = descriptorForTypeId(true, typeId, sysLdr, deserialize)) != null)
                return desc;

            throw new BinaryInvalidTypeException(e);
        }
        catch (IgniteCheckedException e) {
            // Class might have been loaded by default class loader.
            if (userType && !ldr.equals(sysLdr) && (desc = descriptorForTypeId(true, typeId, sysLdr, deserialize)) != null)
                return desc;

            throw new BinaryObjectException("Failed resolve class for ID: " + typeId, e);
        }

        if (desc == null) {
            desc = registerClassDescriptor(cls, deserialize);

            assert desc.typeId() == typeId : "Duplicate typeId [typeId=" + typeId + ", cls=" + cls
                + ", desc=" + desc + "]";
        }

        return desc;
    }

    /**
     * Creates and registers {@link BinaryClassDescriptor} for the given {@code class}.
     *
     * @param cls Class.
     * @return Class descriptor.
     */
    private BinaryClassDescriptor registerClassDescriptor(Class<?> cls, boolean deserialize) {
        BinaryClassDescriptor desc;

        String clsName = cls.getName();

        if (marshCtx.isSystemType(clsName)) {
            BinarySerializer serializer = null;

            if (BINARYLIZABLE_SYS_CLSS.contains(clsName))
                serializer = new BinaryReflectiveSerializer();

            desc = new BinaryClassDescriptor(this,
                cls,
                false,
                clsName.hashCode(),
                clsName,
                null,
                SIMPLE_NAME_LOWER_CASE_MAPPER,
                serializer,
                false,
                true /* registered */
            );

            BinaryClassDescriptor old = descByCls.putIfAbsent(cls, desc);

            if (old != null)
                desc = old;
        }
        else
            desc = registerUserClassDescriptor(cls, deserialize);

        return desc;
    }

    /**
     * Creates and registers {@link BinaryClassDescriptor} for the given user {@code class}.
     *
     * @param cls Class.
     * @return Class descriptor.
     */
    private BinaryClassDescriptor registerUserClassDescriptor(Class<?> cls, boolean deserialize) {
        boolean registered;

        final String clsName = cls.getName();

        BinaryInternalMapper mapper = userTypeMapper(clsName);

        final String typeName = mapper.typeName(clsName);

        final int typeId = mapper.typeId(clsName);

        try {
            registered = marshCtx.registerClass(typeId, cls);
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException("Failed to register class.", e);
        }

        BinarySerializer serializer = serializerForClass(cls);

        String affFieldName = affinityFieldName(cls);

        BinaryClassDescriptor desc = new BinaryClassDescriptor(this,
            cls,
            true,
            typeId,
            typeName,
            affFieldName,
            mapper,
            serializer,
            true,
            registered
        );

        if (!deserialize)
            metaHnd.addMeta(typeId,
                new BinaryMetadata(typeId, typeName, desc.fieldsMeta(), affFieldName, null, desc.isEnum()).wrap(this));

        descByCls.put(cls, desc);

        typeId2Mapper.putIfAbsent(typeId, mapper);

        return desc;
    }

    /**
     * Creates and registers {@link BinaryClassDescriptor} for the given user {@code class}.
     *
     * @param desc Old descriptor that should be re-registered.
     * @return Class descriptor.
     */
    private BinaryClassDescriptor registerUserClassDescriptor(BinaryClassDescriptor desc) {
        boolean registered;

        try {
            registered = marshCtx.registerClass(desc.typeId(), desc.describedClass());
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException("Failed to register class.", e);
        }

        if (registered) {
            BinarySerializer serializer = desc.initialSerializer();

            if (serializer == null)
                serializer = serializerForClass(desc.describedClass());

            desc = new BinaryClassDescriptor(
                this,
                desc.describedClass(),
                true,
                desc.typeId(),
                desc.typeName(),
                desc.affFieldKeyName(),
                desc.mapper(),
                serializer,
                true,
                true
            );

            descByCls.put(desc.describedClass(), desc);
        }

        return desc;
    }

    /**
     * Get serializer for class taking in count default one.
     *
     * @param cls Class.
     * @return Serializer for class or {@code null} if none exists.
     */
    private @Nullable BinarySerializer serializerForClass(Class cls) {
        BinarySerializer serializer = defaultSerializer();

        if (serializer == null && canUseReflectiveSerializer(cls))
            serializer = new BinaryReflectiveSerializer();

        return serializer;
    }

    /**
     * @return Default serializer.
     */
    private BinarySerializer defaultSerializer() {
        BinaryConfiguration binCfg = igniteCfg.getBinaryConfiguration();

        return binCfg != null ? binCfg.getSerializer() : null;
    }

    /**
     * @param cls Collection class.
     * @return Collection type ID.
     */
    public byte collectionType(Class<? extends Collection> cls) {
        assert cls != null;

        Byte type = colTypes.get(cls);

        if (type != null)
            return type;

        return Set.class.isAssignableFrom(cls) ? GridBinaryMarshaller.USER_SET : GridBinaryMarshaller.USER_COL;
    }

    /**
     * @param cls Map class.
     * @return Map type ID.
     */
    public byte mapType(Class<? extends Map> cls) {
        assert cls != null;

        Byte type = mapTypes.get(cls);

        return type != null ? type : GridBinaryMarshaller.USER_COL;
    }

    /**
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName) {
        Integer id = predefinedTypeNames.get(SIMPLE_NAME_LOWER_CASE_MAPPER.typeName(typeName));

        if (id != null)
            return id;

        if (marshCtx.isSystemType(typeName))
            return typeName.hashCode();

        BinaryInternalMapper mapper = userTypeMapper(typeName);

        return mapper.typeId(typeName);
    }

    /**
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    public int fieldId(int typeId, String fieldName) {
        BinaryInternalMapper mapper = userTypeMapper(typeId);

        return mapper.fieldId(typeId, fieldName);
    }

    /**
     * @param typeId Type ID.
     * @return Instance of ID mapper.
     */
    BinaryInternalMapper userTypeMapper(int typeId) {
        BinaryInternalMapper mapper = typeId2Mapper.get(typeId);

        return mapper != null ? mapper : SIMPLE_NAME_LOWER_CASE_MAPPER;
    }

    /**
     * @param clsName Type name.
     * @return Instance of ID mapper.
     */
    BinaryInternalMapper userTypeMapper(String clsName) {
        BinaryInternalMapper mapper = cls2Mappers.get(clsName);

        if (mapper != null)
            return mapper;

        mapper = resolveMapper(clsName, igniteCfg.getBinaryConfiguration());

        BinaryInternalMapper prevMap = cls2Mappers.putIfAbsent(clsName, mapper);

        if (prevMap != null && !mapper.equals(prevMap))
            throw new IgniteException("Different mappers [clsName=" + clsName + ", newMapper=" + mapper
                + ", prevMap=" + prevMap + "]");

        prevMap = typeId2Mapper.putIfAbsent(mapper.typeId(clsName), mapper);

        if (prevMap != null && !mapper.equals(prevMap))
            throw new IgniteException("Different mappers [clsName=" + clsName + ", newMapper=" + mapper
                + ", prevMap=" + prevMap + "]");

        return mapper;
    }

    /**
     * @param clsName Type name.
     * @param cfg Binary configuration.
     * @return Mapper according to configuration.
     */
    private static BinaryInternalMapper resolveMapper(String clsName, BinaryConfiguration cfg) {
        assert clsName != null;

        if (cfg == null)
            return DFLT_MAPPER;

        BinaryIdMapper globalIdMapper = cfg.getIdMapper();
        BinaryNameMapper globalNameMapper = cfg.getNameMapper();

        Collection<BinaryTypeConfiguration> typeCfgs = cfg.getTypeConfigurations();

        if (typeCfgs != null) {
            for (BinaryTypeConfiguration typeCfg : typeCfgs) {
                String typeCfgName = typeCfg.getTypeName();

                // Pattern.
                if (typeCfgName != null && typeCfgName.endsWith(".*")) {
                    String pkgName = typeCfgName.substring(0, typeCfgName.length() - 2);

                    int dotIndex = clsName.lastIndexOf('.');

                    if (dotIndex > 0) {
                        String typePkgName = clsName.substring(0, dotIndex);

                        if (pkgName.equals(typePkgName)) {
                            // Resolve mapper.
                            BinaryIdMapper idMapper = globalIdMapper;

                            if (typeCfg.getIdMapper() != null)
                                idMapper = typeCfg.getIdMapper();

                            BinaryNameMapper nameMapper = globalNameMapper;

                            if (typeCfg.getNameMapper() != null)
                                nameMapper = typeCfg.getNameMapper();

                            return resolveMapper(nameMapper, idMapper);
                        }
                    }
                }
            }
        }

        return resolveMapper(globalNameMapper, globalIdMapper);
    }

    /**
     * @param clsName Class name.
     * @return Type name.
     */
    public String userTypeName(String clsName) {
        BinaryInternalMapper mapper = userTypeMapper(clsName);

        return mapper.typeName(clsName);
    }

    /**
     * @param cls Class to get affinity field for.
     * @return Affinity field name or {@code null} if field name was not found.
     */
    private String affinityFieldName(Class cls) {
        for (; cls != Object.class && cls != null; cls = cls.getSuperclass()) {
            for (Field f : cls.getDeclaredFields()) {
                if (f.getAnnotation(AffinityKeyMapped.class) != null)
                    return f.getName();
            }
        }

        return null;
    }

    /**
     * @param cls Class.
     * @param id Type ID.
     * @return GridBinaryClassDescriptor.
     */
    public BinaryClassDescriptor registerPredefinedType(Class<?> cls, int id) {
        return registerPredefinedType(cls, id, null, true);
    }

    /**
     * @param cls Class.
     * @param id Type ID.
     * @param affFieldName Affinity field name.
     * @return GridBinaryClassDescriptor.
     */
    public BinaryClassDescriptor registerPredefinedType(Class<?> cls, int id, String affFieldName, boolean registered) {
        String simpleClsName = SIMPLE_NAME_LOWER_CASE_MAPPER.typeName(cls.getName());

        if (id == 0)
            id = SIMPLE_NAME_LOWER_CASE_MAPPER.typeId(simpleClsName);

        BinaryClassDescriptor desc = new BinaryClassDescriptor(
            this,
            cls,
            false,
            id,
            simpleClsName,
            affFieldName,
            SIMPLE_NAME_LOWER_CASE_MAPPER,
            new BinaryReflectiveSerializer(),
            false,
            registered /* registered */
        );

        predefinedTypeNames.put(simpleClsName, id);
        predefinedTypes.put(id, desc);

        descByCls.put(cls, desc);

        if (affFieldName != null)
            affKeyFieldNames.putIfAbsent(id, affFieldName);

        return desc;
    }

    /**
     * @param clsName Class name.
     * @param mapper ID mapper.
     * @param serializer Serializer.
     * @param identity Type identity.
     * @param affKeyFieldName Affinity key field name.
     * @param isEnum If enum.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("ErrorNotRethrown")
    public void registerUserType(String clsName,
        BinaryInternalMapper mapper,
        @Nullable BinarySerializer serializer,
        @Nullable BinaryIdentityResolver identity,
        @Nullable String affKeyFieldName,
        boolean isEnum)
        throws BinaryObjectException {
        assert mapper != null;

        Class<?> cls = null;

        try {
            cls = U.resolveClassLoader(configuration()).loadClass(clsName);
        }
        catch (ClassNotFoundException | NoClassDefFoundError ignored) {
            // No-op.
        }

        String typeName = mapper.typeName(clsName);

        int id = mapper.typeId(clsName);

        //Workaround for IGNITE-1358
        if (predefinedTypes.get(id) != null)
            throw duplicateTypeIdException(clsName, id);

        if (typeId2Mapper.put(id, mapper) != null)
            throw duplicateTypeIdException(clsName, id);

        if (identity != null) {
            if (identities.put(id, identity) != null)
                throw duplicateTypeIdException(clsName, id);
        }

        if (affKeyFieldName != null) {
            if (affKeyFieldNames.put(id, affKeyFieldName) != null)
                throw duplicateTypeIdException(clsName, id);
        }

        cls2Mappers.put(clsName, mapper);

        Map<String, Integer> fieldsMeta = null;

        if (cls != null) {
            if (serializer == null) {
                // At this point we must decide whether to rely on Java serialization mechanics or not.
                // If no serializer is provided, we examine the class and if it doesn't contain non-trivial
                // serialization logic we are safe to fallback to reflective binary serialization.
                if (canUseReflectiveSerializer(cls))
                    serializer = new BinaryReflectiveSerializer();
            }

            BinaryClassDescriptor desc = new BinaryClassDescriptor(
                this,
                cls,
                true,
                id,
                typeName,
                affKeyFieldName,
                mapper,
                serializer,
                true,
                true
            );

            fieldsMeta = desc.fieldsMeta();

            descByCls.put(cls, desc);

            // Registering in order to support the interoperability between Java, C++ and .Net.
            // https://issues.apache.org/jira/browse/IGNITE-3455
            predefinedTypes.put(id, desc);
        }

        metaHnd.addMeta(id, new BinaryMetadata(id, typeName, fieldsMeta, affKeyFieldName, null, isEnum).wrap(this));
    }

    /**
     * Throw exception on class duplication.
     *
     * @param clsName Class name.
     * @param id Type id.
     */
    private static BinaryObjectException duplicateTypeIdException(String clsName, int id) {
        return new BinaryObjectException("Duplicate type ID [clsName=" + clsName + ", id=" + id + ']');
    }

    /**
     * Check whether reflective serializer can be used for class.
     *
     * @param cls Class.
     * @return {@code True} if reflective serializer can be used.
     */
    private static boolean canUseReflectiveSerializer(Class cls) {
        return BinaryUtils.isBinarylizable(cls) || !BinaryUtils.isCustomJavaSerialization(cls);
    }

    /**
     * Create binary field.
     *
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Binary field.
     */
    public BinaryFieldImpl createField(int typeId, String fieldName) {
        BinarySchemaRegistry schemaReg = schemaRegistry(typeId);

        BinaryInternalMapper mapper = userTypeMapper(typeId);

        int fieldId = mapper.fieldId(typeId, fieldName);

        return new BinaryFieldImpl(typeId, schemaReg, fieldName, fieldId);
    }

    /**
     * @param typeId Type ID.
     * @return Meta data.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public BinaryType metadata(int typeId) throws BinaryObjectException {
        return metaHnd != null ? metaHnd.metadata(typeId) : null;
    }

    /**
     * @param typeId Type ID.
     * @return Affinity key field name.
     */
    public String affinityKeyFieldName(int typeId) {
        return affKeyFieldNames.get(typeId);
    }

    /**
     * @param typeId Type ID.
     * @return Type identity.
     */
    public BinaryIdentityResolver identity(int typeId) {
        return identities.get(typeId);
    }

    /**
     * @param typeId Type ID.
     * @param meta Meta data.
     * @throws BinaryObjectException In case of error.
     */
    public void updateMetadata(int typeId, BinaryMetadata meta) throws BinaryObjectException {
        metaHnd.addMeta(typeId, meta.wrap(this));
    }

    /**
     * @return Whether field IDs should be skipped in footer or not.
     */
    public boolean isCompactFooter() {
        return compactFooter;
    }

    /**
     * Get schema registry for type ID.
     *
     * @param typeId Type ID.
     * @return Schema registry for type ID.
     */
    public BinarySchemaRegistry schemaRegistry(int typeId) {
        Map<Integer, BinarySchemaRegistry> schemas0 = schemas;

        if (schemas0 == null) {
            synchronized (this) {
                schemas0 = schemas;

                if (schemas0 == null) {
                    schemas0 = new HashMap<>();

                    BinarySchemaRegistry reg = new BinarySchemaRegistry();

                    schemas0.put(typeId, reg);

                    schemas = schemas0;

                    return reg;
                }
            }
        }

        BinarySchemaRegistry reg = schemas0.get(typeId);

        if (reg == null) {
            synchronized (this) {
                reg = schemas.get(typeId);

                if (reg == null) {
                    reg = new BinarySchemaRegistry();

                    schemas0 = new HashMap<>(schemas);

                    schemas0.put(typeId, reg);

                    schemas = schemas0;
                }
            }
        }

        return reg;
    }

    /**
     * Unregister all binary schemas.
     */
    public void unregisterBinarySchemas() {
        schemas = null;
    }

    /**
     * Returns instance of {@link OptimizedMarshaller}.
     *
     * @return Optimized marshaller.
     */
    OptimizedMarshaller optimizedMarsh() {
        return optmMarsh;
    }

    /**
     * Undeployment callback invoked when class loader is being undeployed.
     *
     * Some marshallers may want to clean their internal state that uses the undeployed class loader somehow.
     *
     * @param ldr Class loader being undeployed.
     */
    public void onUndeploy(ClassLoader ldr) {
        for (Class<?> cls : descByCls.keySet()) {
            if (ldr.equals(cls.getClassLoader()))
                descByCls.remove(cls);
        }

        U.clearClassCache(ldr);
    }

    /**
     * Type descriptors.
     */
    private static class TypeDescriptors {
        /** Descriptors map. */
        private final Map<String, TypeDescriptor> descs = new LinkedHashMap<>();

        /**
         * Add type descriptor.
         *
         * @param clsName Class name.
         * @param mapper Mapper.
         * @param serializer Serializer.
         * @param identity Key hashing mode.
         * @param affKeyFieldName Affinity key field name.
         * @param isEnum Enum flag.
         * @param canOverride Whether this descriptor can be override.
         * @throws BinaryObjectException If failed.
         */
        private void add(String clsName,
            BinaryInternalMapper mapper,
            BinarySerializer serializer,
            BinaryIdentityResolver identity,
            String affKeyFieldName,
            boolean isEnum,
            boolean canOverride)
            throws BinaryObjectException {
            TypeDescriptor desc = new TypeDescriptor(clsName,
                mapper,
                serializer,
                identity,
                affKeyFieldName,
                isEnum,
                canOverride);

            TypeDescriptor oldDesc = descs.get(clsName);

            if (oldDesc == null)
                descs.put(clsName, desc);
            else
                oldDesc.override(desc);
        }

        /**
         * Get all collected descriptors.
         *
         * @return Descriptors.
         */
        private Iterable<TypeDescriptor> descriptors() {
            return descs.values();
        }
    }

    /**
     * Type descriptor.
     */
    private static class TypeDescriptor {
        /** Class name. */
        private final String clsName;

        /** Mapper. */
        private BinaryInternalMapper mapper;

        /** Serializer. */
        private BinarySerializer serializer;

        /** Type identity. */
        private BinaryIdentityResolver identity;

        /** Affinity key field name. */
        private String affKeyFieldName;

        /** Enum flag. */
        private boolean isEnum;

        /** Whether this descriptor can be override. */
        private boolean canOverride;

        /**
         * Constructor.
         * @param clsName Class name.
         * @param mapper ID mapper.
         * @param serializer Serializer.
         * @param identity Key hashing mode.
         * @param affKeyFieldName Affinity key field name.
         * @param isEnum Enum type.
         * @param canOverride Whether this descriptor can be override.
         */
        private TypeDescriptor(String clsName, BinaryInternalMapper mapper,
            BinarySerializer serializer, BinaryIdentityResolver identity, String affKeyFieldName, boolean isEnum,
            boolean canOverride) {
            this.clsName = clsName;
            this.mapper = mapper;
            this.serializer = serializer;
            this.identity = identity;
            this.affKeyFieldName = affKeyFieldName;
            this.isEnum = isEnum;
            this.canOverride = canOverride;
        }

        /**
         * Override binary class descriptor.
         *
         * @param other Other descriptor.
         * @throws BinaryObjectException If failed.
         */
        private void override(TypeDescriptor other) throws BinaryObjectException {
            assert clsName.equals(other.clsName);

            if (canOverride) {
                mapper = other.mapper;
                serializer = other.serializer;
                identity = other.identity;
                affKeyFieldName = other.affKeyFieldName;
                isEnum = other.isEnum;
                canOverride = other.canOverride;
            }
            else if (!other.canOverride)
                throw new BinaryObjectException("Duplicate explicit class definition in configuration: " + clsName);
        }
    }

    /**
     * Type id wrapper.
     */
    static class Type {
        /** Type id */
        private final int id;

        /** Whether the following type is registered in a cache or not */
        private final boolean registered;

        /**
         * @param id Id.
         * @param registered Registered.
         */
        public Type(int id, boolean registered) {
            this.id = id;
            this.registered = registered;
        }

        /**
         * @return Type ID.
         */
        public int id() {
            return id;
        }

        /**
         * @return Registered flag value.
         */
        public boolean registered() {
            return registered;
        }
    }
}
