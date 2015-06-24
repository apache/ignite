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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.indexing.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.IgniteComponentType.*;
import static org.apache.ignite.internal.processors.query.GridQueryIndexType.*;

/**
 * Indexing processor.
 */
public class GridQueryProcessor extends GridProcessorAdapter {
    /** For tests. */
    public static Class<? extends GridQueryIndexing> idxCls;

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Type descriptors. */
    private final Map<TypeId, TypeDescriptor> types = new ConcurrentHashMap8<>();

    /** Type descriptors. */
    private final ConcurrentMap<TypeName, TypeDescriptor> typesByName = new ConcurrentHashMap8<>();

    /** */
    private ExecutorService execSvc;

    /** */
    private final GridQueryIndexing idx;

    /**
     * @param ctx Kernal context.
     */
    public GridQueryProcessor(GridKernalContext ctx) throws IgniteCheckedException {
        super(ctx);

        if (idxCls != null) {
            idx = U.newInstance(idxCls);

            idxCls = null;
        }
        else
            idx = INDEXING.inClassPath() ? U.<GridQueryIndexing>newInstance(INDEXING.className()) : null;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        if (idx != null) {
            ctx.resource().injectGeneric(idx);

            execSvc = ctx.getExecutorService();

            idx.start(ctx, busyLock);
        }
    }

    /**
     * @param ccfg Cache configuration.
     * @return {@code true} If query index must be enabled for this cache.
     */
    public static boolean isEnabled(CacheConfiguration<?,?> ccfg) {
        return !F.isEmpty(ccfg.getIndexedTypes()) || !F.isEmpty(ccfg.getTypeMetadata());
    }

    /**
     * @return {@code true} If indexing module is in classpath and successfully initialized.
     */
    public boolean moduleEnabled() {
        return idx != null;
    }

    /**
     * @param ccfg Cache configuration.
     * @throws IgniteCheckedException If failed.
     */
    public void initializeCache(CacheConfiguration<?, ?> ccfg) throws IgniteCheckedException {
        idx.registerCache(ccfg);

        try {
            if (!F.isEmpty(ccfg.getTypeMetadata()))
                processTypeMetadata(ccfg);
        }
        catch (IgniteCheckedException | RuntimeException e) {
            idx.unregisterCache(ccfg);

            throw e;
        }
    }

    /**
     * Generates cache type metadata if indexed types are defined.
     *
     * @param ccfg Cache configuration.
     * @throws IgniteCheckedException In case of error.
     */
    public void generateTypeMetadata(CacheConfiguration<?, ?> ccfg) throws IgniteCheckedException {
        if (ccfg.getTypeMetadata() == null) {
            Class<?>[] clss = ccfg.getIndexedTypes();

            if (!F.isEmpty(clss)) {
                List<CacheTypeMetadata> metadata = new ArrayList<>(clss.length / 2);

                for (int i = 0; i < clss.length; i += 2) {
                    Class<?> keyCls = clss[i];
                    Class<?> valCls = clss[i + 1];

                    CacheTypeMetadata meta = new CacheTypeMetadata();

                    meta.setKeyType(keyCls);
                    meta.setValueType(valCls);

                    Map<String, TreeMap<Integer, T3<String, Class<?>, Boolean>>> orderedGroups = new HashMap<>();

                    processClassAnnotations(keyCls, meta, null, orderedGroups);
                    processClassAnnotations(valCls, meta, null, orderedGroups);

                    fillOrderedGroups(meta, orderedGroups);

                    metadata.add(meta);
                }

                ccfg.setTypeMetadata(metadata);
            }
        }

        if (ctx.cacheObjects().isPortableEnabled() && ccfg.getTypeMetadata() != null)
            maskClasses(ccfg.getTypeMetadata());
    }

    /**
     * @param metadata Metadata.
     */
    private static void maskClasses(Collection<CacheTypeMetadata> metadata) {
        for (CacheTypeMetadata meta : metadata) {
            maskFieldsTypes(meta.getQueryFields());

            maskFieldsTypes(meta.getAscendingFields());

            maskFieldsTypes(meta.getDescendingFields());

            for (LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> grp : meta.getGroups().values()) {
                for (Map.Entry<String, IgniteBiTuple<Class<?>, Boolean>> entry : grp.entrySet()) {
                    Class<?> cls = entry.getValue().get1();

                    if (!U.isJdk(cls))
                        entry.getValue().set1(Object.class);
                }
            }
        }
    }

    /**
     * @param fields Fields.
     */
    private static void maskFieldsTypes(Map<String, Class<?>> fields) {
        for (Map.Entry<String, Class<?>> entry : fields.entrySet())
            if (!U.isJdk(entry.getValue()))
                entry.setValue(Objects.class);
    }

    /**
     * @param meta Cache type metadata.
     * @param orderedGroups Ordered groups.
     * @throws IgniteCheckedException In case or error.
     */
    private static void fillOrderedGroups(CacheTypeMetadata meta,
        Map<String, TreeMap<Integer, T3<String, Class<?>, Boolean>>> orderedGroups)
        throws IgniteCheckedException
    {
        for (Map.Entry<String, TreeMap<Integer, T3<String, Class<?>, Boolean>>> ordGrp : orderedGroups.entrySet()) {
            String grpName = ordGrp.getKey();

            Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> groups = meta.getGroups();

            LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> fields = groups.get(grpName);

            if (fields == null)
                groups.put(grpName, fields = new LinkedHashMap<>());

            for (Map.Entry<Integer, T3<String, Class<?>, Boolean>> grpFields : ordGrp.getValue().entrySet()) {
                String name = grpFields.getValue().get1();

                Class<?> cls = grpFields.getValue().get2();

                Boolean desc = grpFields.getValue().get3();

                if (fields.put(name, new T2<Class<?>, Boolean>(cls, desc)) != null)
                    throw new IgniteCheckedException("Field " + name + " already exists in group " + grpName);
            }
        }
    }

    /**
     * @param cls Class.
     * @param meta Type metadata.
     * @param parentField Parent field name.
     */
    private void processClassAnnotations(Class<?> cls, CacheTypeMetadata meta,
        String parentField, Map<String, TreeMap<Integer, T3<String, Class<?>, Boolean>>> orderedGroups)
        throws IgniteCheckedException
    {
        if (U.isJdk(cls) || idx.isGeometryClass(cls))
            return;

        for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                QueryTextField txtAnn = field.getAnnotation(QueryTextField.class);

                if (txtAnn != null) {
                    String fieldName = parentField == null ? field.getName() : parentField + '.' + field.getName();

                    meta.getTextFields().add(fieldName);

                    meta.getQueryFields().put(fieldName, field.getType());
                }

                QuerySqlField sqlAnn = field.getAnnotation(QuerySqlField.class);

                if (sqlAnn != null) {
                    String name = sqlAnn.name().isEmpty() ? field.getName() : sqlAnn.name();

                    String pathStr = parentField == null ? name : parentField + '.' + name;

                    processClassAnnotations(field.getType(), meta, pathStr, orderedGroups);

                    if (sqlAnn.index()) {
                        Map<String, Class<?>> fields =
                            sqlAnn.descending() ? meta.getDescendingFields() : meta.getAscendingFields();

                        fields.put(pathStr, field.getType());
                    }

                    meta.getQueryFields().put(pathStr, field.getType());

                    if (!sqlAnn.name().isEmpty())
                        meta.addAlias(sqlAnn.name(), field.getName());

                    if (!F.isEmpty(sqlAnn.groups())) {
                        for (String grp : sqlAnn.groups()) {
                            LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> fields = meta.getGroups().get(grp);

                            if (fields == null)
                                meta.getGroups().put(grp, fields = new LinkedHashMap<>());

                            IgniteBiTuple<Class<?>, Boolean> fieldType =
                                new IgniteBiTuple<Class<?>, Boolean>(field.getType(), false);

                            if (fields.put(pathStr, fieldType) != null)
                                throw new IgniteCheckedException("Field " + pathStr +
                                    " already exists in group " + grp);
                        }
                    }

                    if (!F.isEmpty(sqlAnn.orderedGroups())) {
                        for (QuerySqlField.Group idx : sqlAnn.orderedGroups()) {
                            TreeMap<Integer, T3<String, Class<?>, Boolean>> orderedFields =
                                orderedGroups.get(idx.name());

                            if (orderedFields == null)
                                orderedGroups.put(idx.name(), orderedFields = new TreeMap<>());

                            T3<String, Class<?>, Boolean> grp =
                                new T3<String, Class<?>, Boolean>(pathStr, field.getType(), idx.descending());

                            if (orderedFields.put(idx.order(), grp) != null)
                                throw new IgniteCheckedException("Field " + pathStr + " has duplicated order " +
                                    idx.order() + " in group " + idx.name());
                        }
                    }
                }
            }

            for (Method mtd : c.getDeclaredMethods()) {
                String mtdName = mtd.getName().startsWith("get") && mtd.getName().length() > 3 ?
                    mtd.getName().substring(3) : mtd.getName();

                QuerySqlField sqlAnn = mtd.getAnnotation(QuerySqlField.class);
                QueryTextField txtAnn = mtd.getAnnotation(QueryTextField.class);

                if (sqlAnn != null || txtAnn != null) {
                    if (mtd.getParameterTypes().length != 0)
                        throw new IgniteCheckedException("Getter with QuerySqlField " +
                            "annotation cannot have parameters: " + mtd);

                    Class<?> type = mtd.getReturnType();

                    if (txtAnn != null) {
                        String pathStr = parentField == null ? mtdName : parentField + '.' + mtdName;

                        meta.getTextFields().add(pathStr);

                        meta.getQueryFields().put(pathStr, type);
                    }

                    if (sqlAnn != null) {
                        String name = sqlAnn.name().isEmpty() ? mtdName : sqlAnn.name();

                        name = parentField == null ? name : parentField + '.' + name;

                        processClassAnnotations(mtd.getReturnType(), meta, name, orderedGroups);

                        if (sqlAnn.index()) {
                            Map<String, Class<?>> fields =
                                sqlAnn.descending() ? meta.getDescendingFields() : meta.getAscendingFields();

                            fields.put(name, type);
                        }

                        meta.getQueryFields().put(name, type);

                        if (!sqlAnn.name().isEmpty())
                            meta.addAlias(sqlAnn.name(), mtdName);

                        if (!F.isEmpty(sqlAnn.groups())) {
                            for (String grp : sqlAnn.groups()) {
                                LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> fields =
                                    meta.getGroups().get(grp);

                                if (fields == null) {
                                    fields = new LinkedHashMap<>();

                                    meta.getGroups().put(grp, fields);
                                }

                                IgniteBiTuple<Class<?>, Boolean> fieldType =
                                    new IgniteBiTuple<Class<?>, Boolean>(mtd.getReturnType(), false);

                                if (fields.put(name, fieldType) != null)
                                    throw new IgniteCheckedException("Field " + name +
                                        " already exists in group " + grp);
                            }
                        }

                        if (!F.isEmpty(sqlAnn.orderedGroups())) {
                            for (QuerySqlField.Group idx : sqlAnn.orderedGroups()) {
                                TreeMap<Integer, T3<String, Class<?>, Boolean>> orderedFields =
                                    orderedGroups.get(idx.name());

                                if (orderedFields == null)
                                    orderedGroups.put(idx.name(), orderedFields = new TreeMap<>());

                                T3<String, Class<?>, Boolean> grp =
                                    new T3<String, Class<?>, Boolean>(name, mtd.getReturnType(), idx.descending());

                                if (orderedFields.put(idx.order(), grp) != null)
                                    throw new IgniteCheckedException("Field " + name + " has duplicated order " +
                                        idx.order() + " in group " + idx.name());
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * @param ccfg Cache configuration.
     */
    private void processTypeMetadata(CacheConfiguration<?, ?> ccfg) throws IgniteCheckedException {
        for (CacheTypeMetadata meta : ccfg.getTypeMetadata()) {
            if (F.isEmpty(meta.getValueType()))
                throw new IgniteCheckedException("Value type is not set: " + meta);

            TypeDescriptor desc = new TypeDescriptor();

            Class<?> valCls = U.classForName(meta.getValueType(), null);

            //desc.name(valCls != null ? typeName(valCls) : meta.getValueType());
            desc.name(meta.getValTypeName());

            desc.valueClass(valCls != null ? valCls : Object.class);
            desc.keyClass(
                meta.getKeyType() == null ?
                    Object.class :
                    U.classForName(meta.getKeyType(), Object.class));

            TypeId typeId;

            if (valCls == null || ctx.cacheObjects().isPortableEnabled()) {
                processPortableMeta(meta, desc);

                typeId = new TypeId(ccfg.getName(), ctx.cacheObjects().typeId(meta.getValueType()));
            }
            else {
                processClassMeta(meta, desc);

                typeId = new TypeId(ccfg.getName(), valCls);

                // We have to index primitive _val.
                if ((U.isJdk(valCls) || idx.isGeometryClass(valCls)) && idx.isSqlType(valCls)) {
                    String idxName = "_val_idx";

                    desc.addIndex(idxName, idx.isGeometryClass(valCls) ? GEO_SPATIAL : SORTED);

                    desc.addFieldToIndex(idxName, "_VAL", 0, false);
                }
            }

            addTypeByName(ccfg, desc);
            types.put(typeId, desc);

            desc.registered(idx.registerType(ccfg.getName(), desc));
        }
    }

    /**
     * @param ccfg Cache configuration.
     * @param desc Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private void addTypeByName(CacheConfiguration<?,?> ccfg, TypeDescriptor desc) throws IgniteCheckedException {
        if (typesByName.putIfAbsent(new TypeName(ccfg.getName(), desc.name()), desc) != null)
            throw new IgniteCheckedException("Type with name '" + desc.name() + "' already indexed " +
                "in cache '" + ccfg.getName() + "'.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        busyLock.block();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        if (idx != null)
            idx.stop();
    }

    /**
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheStart(GridCacheContext cctx) throws IgniteCheckedException {
        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            return;

        try {
            initializeCache(cctx.config());
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cctx Cache context.
     */
    public void onCacheStop(GridCacheContext cctx) {
        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            return;

        try {
            idx.unregisterCache(cctx.config());

            Iterator<Map.Entry<TypeId, TypeDescriptor>> it = types.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<TypeId, TypeDescriptor> entry = it.next();

                if (F.eq(cctx.name(), entry.getKey().space)) {
                    it.remove();

                    typesByName.remove(new TypeName(cctx.name(), entry.getValue().name()));
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to clear indexing on cache stop (will ignore): " + cctx.name(), e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Returns number of objects of given type for given space of spi.
     *
     * @param space Space.
     * @param valType Value type.
     * @return Objects number or -1 if this type is unknown for given SPI and space.
     * @throws IgniteCheckedException If failed.
     */
    public long size(@Nullable String space, Class<?> valType) throws IgniteCheckedException {
        checkEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to get space size (grid is stopping).");

        try {
            TypeDescriptor desc = types.get(new TypeId(space, valType));

            if (desc == null || !desc.registered())
                return -1;

            return idx.size(space, desc, null);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Rebuilds all search indexes of given value type for given space of spi.
     *
     * @param space Space.
     * @param valTypeName Value type name.
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    public IgniteInternalFuture<?> rebuildIndexes(@Nullable final String space, String valTypeName) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to rebuild indexes (grid is stopping).");

        try {
            return rebuildIndexes(
                space,
                typesByName.get(
                    new TypeName(
                        space,
                        valTypeName)));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param space Space.
     * @param desc Type descriptor.
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    private IgniteInternalFuture<?> rebuildIndexes(@Nullable final String space, @Nullable final TypeDescriptor desc) {
        if (idx == null)
            return new GridFinishedFuture<>(new IgniteCheckedException("Indexing is disabled."));

        if (desc == null || !desc.registered())
            return new GridFinishedFuture<Void>();

        final GridWorkerFuture<?> fut = new GridWorkerFuture<Void>();

        GridWorker w = new GridWorker(ctx.gridName(), "index-rebuild-worker", log) {
            @Override protected void body() {
                try {
                    idx.rebuildIndexes(space, desc);

                    fut.onDone();
                }
                catch (Exception e) {
                    fut.onDone(e);
                }
                catch (Throwable e) {
                    log.error("Failed to rebuild indexes for type: " + desc.name(), e);

                    fut.onDone(e);

                    if (e instanceof Error)
                        throw e;
                }
            }
        };

        fut.setWorker(w);

        execSvc.execute(w);

        return fut;
    }

    /**
     * Rebuilds all search indexes for given spi.
     *
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> rebuildAllIndexes() {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to get space size (grid is stopping).");

        try {
            GridCompoundFuture<?, ?> fut = new GridCompoundFuture<Object, Object>();

            for (Map.Entry<TypeId, TypeDescriptor> e : types.entrySet())
                fut.add((IgniteInternalFuture)rebuildIndexes(e.getKey().space, e.getValue()));

            fut.markInitialized();

            return fut;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param space Space name.
     * @return Cache object context.
     */
    private CacheObjectContext cacheObjectContext(String space) {
        return ctx.cache().internalCache(space).context().cacheObjectContext();
    }

    /**
     * Writes key-value pair to index.
     *
     * @param space Space.
     * @param key Key.
     * @param val Value.
     * @param ver Cache entry version.
     * @param expirationTime Expiration time or 0 if never expires.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("unchecked")
    public void store(final String space, final CacheObject key, final CacheObject val,
        byte[] ver, long expirationTime) throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        if (log.isDebugEnabled())
            log.debug("Store [space=" + space + ", key=" + key + ", val=" + val + "]");

        CacheObjectContext coctx = null;

        if (ctx.indexing().enabled()) {
            coctx = cacheObjectContext(space);

            ctx.indexing().store(space, key.value(coctx, false), val.value(coctx, false), expirationTime);
        }

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to write to index (grid is stopping).");

        try {
            if (coctx == null)
                coctx = cacheObjectContext(space);

            Class<?> valCls = null;

            TypeId id;

            boolean portableVal = ctx.cacheObjects().isPortableObject(val);

            if (portableVal) {
                int typeId = ctx.cacheObjects().typeId(val);

                id = new TypeId(space, typeId);
            }
            else {
                valCls = val.value(coctx, false).getClass();

                id = new TypeId(space, valCls);
            }

            TypeDescriptor desc = types.get(id);

            if (desc == null || !desc.registered())
                return;

            if (!portableVal && !desc.valueClass().isAssignableFrom(valCls))
                throw new IgniteCheckedException("Failed to update index due to class name conflict" +
                    "(multiple classes with same simple name are stored in the same cache) " +
                    "[expCls=" + desc.valueClass().getName() + ", actualCls=" + valCls.getName() + ']');

            if (!ctx.cacheObjects().isPortableObject(key)) {
                Class<?> keyCls = key.value(coctx, false).getClass();

                if (!desc.keyClass().isAssignableFrom(keyCls))
                    throw new IgniteCheckedException("Failed to update index, incorrect key class [expCls=" +
                        desc.keyClass().getName() + ", actualCls=" + keyCls.getName() + "]");
            }

            idx.store(space, desc, key, val, ver, expirationTime);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void checkEnabled() throws IgniteCheckedException {
        if (idx == null)
            throw new IgniteCheckedException("Indexing is disabled.");
    }

    /**
     * @throws IgniteException If indexing is disabled.
     */
    private void checkxEnabled() throws IgniteException {
        if (idx == null)
            throw new IgniteException("Failed to execute query because indexing is disabled (consider adding module " +
                INDEXING.module() + " to classpath or moving it from 'optional' to 'libs' folder).");
    }

    /**
     * @param space Space.
     * @param clause Clause.
     * @param params Parameters collection.
     * @param resType Result type.
     * @param filters Filters.
     * @return Key/value rows.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> query(final String space, final String clause,
        final Collection<Object> params, final String resType, final IndexingQueryFilter filters)
        throws IgniteCheckedException {
        checkEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            final GridCacheContext<?, ?> cctx = ctx.cache().internalCache(space).context();

            return executeQuery(cctx, new IgniteOutClosureX<GridCloseableIterator<IgniteBiTuple<K, V>>>() {
                @Override public GridCloseableIterator<IgniteBiTuple<K, V>> applyx() throws IgniteCheckedException {
                    TypeDescriptor type = typesByName.get(new TypeName(space, resType));

                    if (type == null || !type.registered())
                        throw new CacheException("Failed to find SQL table for type: " + resType);

                    return idx.query(space, clause, params, type, filters);
                }
            });
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param space Space name.
     * @param qry Query.
     * @return Cursor.
     */
    public Iterable<List<?>> queryTwoStep(String space, final GridCacheTwoStepQuery qry) {
        checkxEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            final GridCacheContext<Object, Object> cctx = ctx.cache().internalCache(space).context();

            return executeQuery(cctx, new IgniteOutClosureX<Iterable<List<?>>>() {
                @Override public Iterable<List<?>> applyx() throws IgniteCheckedException {
                    return idx.queryTwoStep(
                        cctx,
                        qry,
                        cctx.keepPortable());
                }
            });
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @return Cursor.
     */
    public QueryCursor<List<?>> queryTwoStep(final GridCacheContext<?,?> cctx, final SqlFieldsQuery qry) {
        checkxEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            return executeQuery(cctx, new IgniteOutClosureX<QueryCursor<List<?>>>() {
                @Override public QueryCursor<List<?>> applyx() throws IgniteCheckedException {
                    return idx.queryTwoStep(cctx, qry);
                }
            });
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @return Cursor.
     */
    public <K,V> QueryCursor<Cache.Entry<K,V>> queryTwoStep(final GridCacheContext<?,?> cctx, final SqlQuery qry) {
        checkxEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            return executeQuery(cctx, new IgniteOutClosureX<QueryCursor<Cache.Entry<K, V>>>() {
                @Override public QueryCursor<Cache.Entry<K, V>> applyx() throws IgniteCheckedException {
                    return idx.queryTwoStep(cctx, qry);
                }
            });
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @return Cursor.
     */
    public <K, V> Iterator<Cache.Entry<K, V>> queryLocal(final GridCacheContext<?, ?> cctx, final SqlQuery qry) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            return executeQuery(
                cctx,
                new IgniteOutClosureX<Iterator<Cache.Entry<K, V>>>() {
                    @Override public Iterator<Cache.Entry<K, V>> applyx() throws IgniteCheckedException {
                        String space = cctx.name();
                        String type = qry.getType();
                        String sqlQry = qry.getSql();
                        Object[] params = qry.getArgs();

                        TypeDescriptor typeDesc = typesByName.get(
                            new TypeName(
                                space,
                                type));

                        if (typeDesc == null || !typeDesc.registered())
                            throw new CacheException("Failed to find SQL table for type: " + type);

                        final GridCloseableIterator<IgniteBiTuple<K, V>> i = idx.query(
                            space,
                            sqlQry,
                            F.asList(params),
                            typeDesc,
                            idx.backupFilter(null, null, null));

                        sendQueryExecutedEvent(
                            sqlQry,
                            params);

                        return new ClIter<Cache.Entry<K, V>>() {
                            @Override public void close() throws Exception {
                                i.close();
                            }

                            @Override public boolean hasNext() {
                                return i.hasNext();
                            }

                            @Override public Cache.Entry<K, V> next() {
                                IgniteBiTuple<K, V> t = i.next();

                                return new CacheEntryImpl<>(
                                    t.getKey(),
                                    t.getValue());
                            }

                            @Override public void remove() {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }
                });
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param sqlQry Sql query.
     * @param params Params.
     */
    private void sendQueryExecutedEvent(String sqlQry, Object[] params) {
        if (ctx.event().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
            ctx.event().record(new CacheQueryExecutedEvent<>(
                ctx.discovery().localNode(),
                "SQL query executed.",
                EVT_CACHE_QUERY_EXECUTED,
                CacheQueryType.SQL.name(),
                null,
                null,
                sqlQry,
                null,
                null,
                params,
                null,
                null));
        }
    }

    /**
     * Closeable iterator.
     */
    private interface ClIter<X> extends AutoCloseable, Iterator<X> {
        // No-op.
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @return Iterator.
     */
    public QueryCursor<List<?>> queryLocalFields(final GridCacheContext<?,?> cctx, final SqlFieldsQuery qry) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            return executeQuery(cctx, new IgniteOutClosureX<QueryCursor<List<?>>>() {
                @Override public QueryCursor<List<?>> applyx() throws IgniteCheckedException {
                    String space = cctx.name();
                    String sql = qry.getSql();
                    Object[] args = qry.getArgs();

                    final GridQueryFieldsResult res = idx.queryFields(space, sql, F.asList(args),
                        idx.backupFilter(null, null, null));

                    sendQueryExecutedEvent(sql, args);

                    QueryCursorImpl<List<?>> cursor = new QueryCursorImpl<>(new Iterable<List<?>>() {
                        @Override public Iterator<List<?>> iterator() {
                            return new GridQueryCacheObjectsIterator(res.iterator(), cctx, cctx.keepPortable());
                        }
                    });

                    cursor.fieldsMeta(res.metaData());

                    return cursor;
                }
            });
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param space Space.
     * @param key Key.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @SuppressWarnings("unchecked")
    public void remove(String space, CacheObject key, CacheObject val) throws IgniteCheckedException {
        assert key != null;

        if (log.isDebugEnabled())
            log.debug("Remove [space=" + space + ", key=" + key + ", val=" + val + "]");

        if (ctx.indexing().enabled()) {
            CacheObjectContext coctx = cacheObjectContext(space);

            ctx.indexing().remove(space, key.value(coctx, false));
        }

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to remove from index (grid is stopping).");

        try {
            idx.remove(space, key, val);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Gets type name by class.
     *
     * @param cls Class.
     * @return Type name.
     */
    public static String typeName(Class<?> cls) {
        String typeName = cls.getSimpleName();

        // To protect from failure on anonymous classes.
        if (F.isEmpty(typeName)) {
            String pkg = cls.getPackage().getName();

            typeName = cls.getName().substring(pkg.length() + (pkg.isEmpty() ? 0 : 1));
        }

        if (cls.isArray()) {
            assert typeName.endsWith("[]");

            typeName = typeName.substring(0, typeName.length() - 2) + "_array";
        }

        return typeName;
    }

    /**
     * @param space Space.
     * @param clause Clause.
     * @param resType Result type.
     * @param filters Key and value filters.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Key/value rows.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryText(final String space, final String clause,
        final String resType, final IndexingQueryFilter filters) throws IgniteCheckedException {
        checkEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            final GridCacheContext<?, ?> cctx = ctx.cache().internalCache(space).context();

            return executeQuery(cctx, new IgniteOutClosureX<GridCloseableIterator<IgniteBiTuple<K, V>>>() {
                @Override public GridCloseableIterator<IgniteBiTuple<K, V>> applyx() throws IgniteCheckedException {
                    TypeDescriptor type = typesByName.get(new TypeName(space, resType));

                    if (type == null || !type.registered())
                        throw new CacheException("Failed to find SQL table for type: " + resType);

                    return idx.queryText(
                        space,
                        clause,
                        type,
                        filters);
                }
            });
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param space Space name.
     * @param clause Clause.
     * @param params Parameters collection.
     * @param filters Key and value filters.
     * @return Field rows.
     * @throws IgniteCheckedException If failed.
     */
    public GridQueryFieldsResult queryFields(@Nullable final String space, final String clause,
        final Collection<Object> params, final IndexingQueryFilter filters) throws IgniteCheckedException {
        checkEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            final GridCacheContext<?, ?> cctx = ctx.cache().internalCache(space).context();

            return executeQuery(cctx, new IgniteOutClosureX<GridQueryFieldsResult>() {
                @Override public GridQueryFieldsResult applyx() throws IgniteCheckedException {
                    return idx.queryFields(space, clause, params, filters);
                }
            });
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Will be called when entry for key will be swapped.
     *
     * @param spaceName Space name.
     * @param key key.
     * @throws IgniteCheckedException If failed.
     */
    public void onSwap(String spaceName, CacheObject key) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Swap [space=" + spaceName + ", key=" + key + "]");

        if (ctx.indexing().enabled()) {
            CacheObjectContext coctx = cacheObjectContext(spaceName);

            ctx.indexing().onSwap(
                spaceName,
                key.value(
                    coctx,
                    false));
        }

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process swap event (grid is stopping).");

        try {
            idx.onSwap(
                spaceName,
                key);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Will be called when entry for key will be unswapped.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @throws IgniteCheckedException If failed.
     */
    public void onUnswap(String spaceName, CacheObject key, CacheObject val)
        throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Unswap [space=" + spaceName + ", key=" + key + ", val=" + val + "]");

        if (ctx.indexing().enabled()) {
            CacheObjectContext coctx = cacheObjectContext(spaceName);

            ctx.indexing().onUnswap(spaceName, key.value(coctx, false), val.value(coctx, false));
        }

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process swap event (grid is stopping).");

        try {
            idx.onUnswap(spaceName, key, val);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Removes index tables for all classes belonging to given class loader.
     *
     * @param space Space name.
     * @param ldr Class loader to undeploy.
     * @throws IgniteCheckedException If undeploy failed.
     */
    public void onUndeploy(@Nullable String space, ClassLoader ldr) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Undeploy [space=" + space + "]");

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process undeploy event (grid is stopping).");

        try {
            Iterator<Map.Entry<TypeId, TypeDescriptor>> it = types.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<TypeId, TypeDescriptor> e = it.next();

                if (!F.eq(e.getKey().space, space))
                    continue;

                TypeDescriptor desc = e.getValue();

                if (ldr.equals(U.detectClassLoader(desc.valCls)) || ldr.equals(U.detectClassLoader(desc.keyCls))) {
                    idx.unregisterType(e.getKey().space, desc);

                    it.remove();
                }
            }
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Processes declarative metadata for class.
     *
     * @param meta Type metadata.
     * @param d Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private void processClassMeta(CacheTypeMetadata meta, TypeDescriptor d)
        throws IgniteCheckedException {
        Class<?> keyCls = d.keyClass();
        Class<?> valCls = d.valueClass();

        assert keyCls != null;
        assert valCls != null;

        for (Map.Entry<String, Class<?>> entry : meta.getQueryFields().entrySet()) {
            ClassProperty prop = buildClassProperty(
                keyCls,
                valCls,
                entry.getKey(),
                entry.getValue(),
                meta.getAliases());

            d.addProperty(prop, false);
        }

        for (Map.Entry<String, Class<?>> entry : meta.getAscendingFields().entrySet()) {
            String name = entry.getKey();

            ClassProperty prop = buildClassProperty(
                keyCls,
                valCls,
                name,
                entry.getValue(),
                meta.getAliases());

            d.addProperty(prop, false);

            String idxName = prop.name() + "_idx";

            d.addIndex(idxName, idx.isGeometryClass(prop.type()) ? GEO_SPATIAL : SORTED);

            d.addFieldToIndex(idxName, prop.name(), 0, false);
        }

        for (Map.Entry<String, Class<?>> entry : meta.getDescendingFields().entrySet()) {
            ClassProperty prop = buildClassProperty(
                keyCls,
                valCls,
                entry.getKey(),
                entry.getValue(),
                meta.getAliases());

            d.addProperty(prop, false);

            String idxName = prop.name() + "_idx";

            d.addIndex(idxName, idx.isGeometryClass(prop.type()) ? GEO_SPATIAL : SORTED);

            d.addFieldToIndex(idxName, prop.name(), 0, true);
        }

        for (String txtIdx : meta.getTextFields()) {
            ClassProperty prop = buildClassProperty(
                keyCls,
                valCls,
                txtIdx,
                String.class,
                meta.getAliases());

            d.addProperty(prop, false);

            d.addFieldToTextIndex(prop.name());
        }

        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> grps = meta.getGroups();

        if (grps != null) {
            for (Map.Entry<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> entry : grps.entrySet()) {
                String idxName = entry.getKey();

                LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> idxFields = entry.getValue();

                int order = 0;

                for (Map.Entry<String, IgniteBiTuple<Class<?>, Boolean>> idxField : idxFields.entrySet()) {
                    ClassProperty prop = buildClassProperty(
                        keyCls,
                        valCls,
                        idxField.getKey(),
                        idxField.getValue().get1(),
                        meta.getAliases());

                    d.addProperty(prop, false);

                    Boolean descending = idxField.getValue().get2();

                    d.addFieldToIndex(idxName, prop.name(), order, descending != null && descending);

                    order++;
                }
            }
        }
    }

    /**
     * Processes declarative metadata for portable object.
     *
     * @param meta Declared metadata.
     * @param d Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private void processPortableMeta(CacheTypeMetadata meta, TypeDescriptor d)
        throws IgniteCheckedException {
        for (Map.Entry<String, Class<?>> entry : meta.getAscendingFields().entrySet()) {
            PortableProperty prop = buildPortableProperty(entry.getKey(), entry.getValue());

            d.addProperty(prop, false);

            String idxName = prop.name() + "_idx";

            d.addIndex(idxName, idx.isGeometryClass(prop.type()) ? GEO_SPATIAL : SORTED);

            d.addFieldToIndex(idxName, prop.name(), 0, false);
        }

        for (Map.Entry<String, Class<?>> entry : meta.getDescendingFields().entrySet()) {
            PortableProperty prop = buildPortableProperty(entry.getKey(), entry.getValue());

            d.addProperty(prop, false);

            String idxName = prop.name() + "_idx";

            d.addIndex(idxName, idx.isGeometryClass(prop.type()) ? GEO_SPATIAL : SORTED);

            d.addFieldToIndex(idxName, prop.name(), 0, true);
        }

        for (String txtIdx : meta.getTextFields()) {
            PortableProperty prop = buildPortableProperty(txtIdx, String.class);

            d.addProperty(prop, false);

            d.addFieldToTextIndex(prop.name());
        }

        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> grps = meta.getGroups();

        if (grps != null) {
            for (Map.Entry<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> entry : grps.entrySet()) {
                String idxName = entry.getKey();

                LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> idxFields = entry.getValue();

                int order = 0;

                for (Map.Entry<String, IgniteBiTuple<Class<?>, Boolean>> idxField : idxFields.entrySet()) {
                    PortableProperty prop = buildPortableProperty(idxField.getKey(), idxField.getValue().get1());

                    d.addProperty(prop, false);

                    Boolean descending = idxField.getValue().get2();

                    d.addFieldToIndex(idxName, prop.name(), order, descending != null && descending);

                    order++;
                }
            }
        }

        for (Map.Entry<String, Class<?>> entry : meta.getQueryFields().entrySet()) {
            PortableProperty prop = buildPortableProperty(entry.getKey(), entry.getValue());

            if (!d.props.containsKey(prop.name()))
                d.addProperty(prop, false);
        }
    }

    /**
     * Builds portable object property.
     *
     * @param pathStr String representing path to the property. May contains dots '.' to identify
     *      nested fields.
     * @param resType Result type.
     * @return Portable property.
     */
    private PortableProperty buildPortableProperty(String pathStr, Class<?> resType) {
        String[] path = pathStr.split("\\.");

        PortableProperty res = null;

        for (String prop : path)
            res = new PortableProperty(prop, res, resType);

        return res;
    }

    /**
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param pathStr Path string.
     * @param resType Result type.
     * @return Class property.
     * @throws IgniteCheckedException If failed.
     */
    private static ClassProperty buildClassProperty(Class<?> keyCls, Class<?> valCls, String pathStr,
        Class<?> resType, Map<String, String> aliases) throws IgniteCheckedException {
        ClassProperty res = buildClassProperty(
            true,
            keyCls,
            pathStr,
            resType,
            aliases);

        if (res == null) // We check key before value consistently with PortableProperty.
            res = buildClassProperty(false, valCls, pathStr, resType, aliases);

        if (res == null)
            throw new IgniteCheckedException("Failed to initialize property '" + pathStr + "' for " +
                "key class '" + keyCls + "' and value class '" + valCls + "'. " +
                "Make sure that one of these classes contains respective getter method or field.");

        return res;
    }

    /**
     * @param key If this is a key property.
     * @param cls Source type class.
     * @param pathStr String representing path to the property. May contains dots '.' to identify nested fields.
     * @param resType Expected result type.
     * @return Property instance corresponding to the given path.
     * @throws IgniteCheckedException If property cannot be created.
     */
    static ClassProperty buildClassProperty(boolean key, Class<?> cls, String pathStr,
        Class<?> resType, Map<String, String> aliases) throws IgniteCheckedException {
        String[] path = pathStr.split("\\.");

        ClassProperty res = null;

        for (String prop : path) {
            ClassProperty tmp;

            String fieldName = aliases.get(prop);

            if (fieldName == null)
                fieldName = prop;

            try {
                StringBuilder bld = new StringBuilder("get");

                bld.append(fieldName);

                bld.setCharAt(3, Character.toUpperCase(bld.charAt(3)));

                tmp = new ClassProperty(cls.getMethod(bld.toString()), key);
            }
            catch (NoSuchMethodException ignore) {
                try {
                    tmp = new ClassProperty(cls.getDeclaredField(fieldName), key);
                }
                catch (NoSuchFieldException ignored) {
                    return null;
                }
            }

            tmp.name(prop);

            tmp.parent(res);

            cls = tmp.type();

            res = tmp;
        }

        if (!U.box(resType).isAssignableFrom(U.box(res.type())))
            return null;

        return res;
    }

    /**
     * Gets types for space.
     *
     * @param space Space name.
     * @return Descriptors.
     */
    public Collection<GridQueryTypeDescriptor> types(@Nullable String space) {
        Collection<GridQueryTypeDescriptor> spaceTypes = new ArrayList<>(
            Math.min(10, types.size()));

        for (Map.Entry<TypeId, TypeDescriptor> e : types.entrySet()) {
            TypeDescriptor desc = e.getValue();

            if (desc.registered() && F.eq(e.getKey().space, space))
                spaceTypes.add(desc);
        }

        return spaceTypes;
    }

    /**
     * Gets type descriptor for space and type name.
     *
     * @param space Space name.
     * @param typeName Type name.
     * @return Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    public GridQueryTypeDescriptor type(@Nullable String space, String typeName) throws IgniteCheckedException {
        TypeDescriptor type = typesByName.get(new TypeName(space, typeName));

        if (type == null || !type.registered())
            throw new IgniteCheckedException("Failed to find type descriptor for type name: " + typeName);

        return type;
    }

    /**
     * @param cctx Cache context.
     * @param clo Closure.
     */
    private <R> R executeQuery(GridCacheContext<?,?> cctx, IgniteOutClosureX<R> clo)
        throws IgniteCheckedException {
        final long start = U.currentTimeMillis();

        Throwable err = null;

        R res = null;

        try {
            res = clo.apply();

            return res;
        }
        catch (GridClosureException e) {
            err = e.unwrap();

            throw (IgniteCheckedException)err;
        }
        finally {
            GridCacheQueryMetricsAdapter metrics = (GridCacheQueryMetricsAdapter)cctx.queries().metrics();

            onExecuted(cctx, metrics, res, err, start, U.currentTimeMillis() - start, log);
        }
    }

    /**
     * @param cctx Cctx.
     * @param metrics Metrics.
     * @param res Result.
     * @param err Err.
     * @param startTime Start time.
     * @param duration Duration.
     * @param log Logger.
     */
    public static void onExecuted(GridCacheContext<?, ?> cctx, GridCacheQueryMetricsAdapter metrics,
        Object res, Throwable err, long startTime, long duration, IgniteLogger log) {
        boolean fail = err != null;

        // Update own metrics.
        metrics.onQueryExecute(duration, fail);

        // Update metrics in query manager.
        cctx.queries().onMetricsUpdate(duration, fail);

        if (log.isTraceEnabled())
            log.trace("Query execution finished [startTime=" + startTime +
                    ", duration=" + duration + ", fail=" + (err != null) + ", res=" + res + ']');
    }

    /**
     *
     */
    private abstract static class Property {
        /**
         * Gets this property value from the given object.
         *
         * @param key Key.
         * @param val Value.
         * @return Property value.
         * @throws IgniteCheckedException If failed.
         */
        public abstract Object value(Object key, Object val) throws IgniteCheckedException;

        /**
         * @return Property name.
         */
        public abstract String name();

        /**
         * @return Class member type.
         */
        public abstract Class<?> type();
    }

    /**
     * Description of type property.
     */
    private static class ClassProperty extends Property {
        /** */
        private final Member member;

        /** */
        private ClassProperty parent;

        /** */
        private String name;

        /** */
        private boolean field;

        /** */
        private boolean key;

        /**
         * Constructor.
         *
         * @param member Element.
         */
        ClassProperty(Member member, boolean key) {
            this.member = member;
            this.key = key;

            name = member instanceof Method && member.getName().startsWith("get") && member.getName().length() > 3 ?
                member.getName().substring(3) : member.getName();

            ((AccessibleObject) member).setAccessible(true);

            field = member instanceof Field;
        }

        /** {@inheritDoc} */
        @Override public Object value(Object key, Object val) throws IgniteCheckedException {
            Object x = this.key ? key : val;

            if (parent != null)
                x = parent.value(key, val);

            if (x == null)
                return null;

            try {
                if (field) {
                    Field field = (Field)member;

                    return field.get(x);
                }
                else {
                    Method mtd = (Method)member;

                    return mtd.invoke(x);
                }
            }
            catch (Exception e) {
                throw new IgniteCheckedException(e);
            }
        }

        /**
         * @param name Property name.
         */
        public void name(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public Class<?> type() {
            return member instanceof Field ? ((Field)member).getType() : ((Method)member).getReturnType();
        }

        /**
         * @param parent Parent property if this is embeddable element.
         */
        public void parent(ClassProperty parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ClassProperty.class, this);
        }

        /**
         * @param cls Class.
         * @return {@code true} If this property or some parent relates to member of the given class.
         */
        public boolean knowsClass(Class<?> cls) {
            return member.getDeclaringClass() == cls || (parent != null && parent.knowsClass(cls));
        }
    }

    /**
     *
     */
    private class PortableProperty extends Property {
        /** Property name. */
        private String propName;

        /** Parent property. */
        private PortableProperty parent;

        /** Result class. */
        private Class<?> type;

        /** */
        private volatile int isKeyProp;

        /**
         * Constructor.
         *
         * @param propName Property name.
         * @param parent Parent property.
         * @param type Result type.
         */
        private PortableProperty(String propName, PortableProperty parent, Class<?> type) {
            this.propName = propName;
            this.parent = parent;
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public Object value(Object key, Object val) throws IgniteCheckedException {
            Object obj;

            if (parent != null) {
                obj = parent.value(key, val);

                if (obj == null)
                    return null;

                if (!ctx.cacheObjects().isPortableObject(obj))
                    throw new IgniteCheckedException("Non-portable object received as a result of property extraction " +
                        "[parent=" + parent + ", propName=" + propName + ", obj=" + obj + ']');
            }
            else {
                int isKeyProp0 = isKeyProp;

                if (isKeyProp0 == 0) {
                    // Key is allowed to be a non-portable object here.
                    // We check key before value consistently with ClassProperty.
                    if (ctx.cacheObjects().isPortableObject(key) && ctx.cacheObjects().hasField(key, propName))
                        isKeyProp = isKeyProp0 = 1;
                    else if (ctx.cacheObjects().hasField(val, propName))
                        isKeyProp = isKeyProp0 = -1;
                    else {
                        U.warn(log, "Neither key nor value have property " +
                            "[propName=" + propName + ", key=" + key + ", val=" + val + "]");

                        return null;
                    }
                }

                obj = isKeyProp0 == 1 ? key : val;
            }

            return ctx.cacheObjects().field(obj, propName);
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return propName;
        }

        /** {@inheritDoc} */
        @Override public Class<?> type() {
            return type;
        }
    }

    /**
     * Descriptor of type.
     */
    private static class TypeDescriptor implements GridQueryTypeDescriptor {
        /** */
        private String name;

        /** Value field names and types with preserved order. */
        @GridToStringInclude
        private final Map<String, Class<?>> fields = new LinkedHashMap<>();

        /** */
        @GridToStringExclude
        private final Map<String, Property> props = new HashMap<>();

        /** */
        @GridToStringInclude
        private final Map<String, IndexDescriptor> indexes = new HashMap<>();

        /** */
        private IndexDescriptor fullTextIdx;

        /** */
        private Class<?> keyCls;

        /** */
        private Class<?> valCls;

        /** */
        private boolean valTextIdx;

        /** SPI can decide not to register this type. */
        private boolean registered;

        /**
         * @return {@code True} if type registration in SPI was finished and type was not rejected.
         */
        boolean registered() {
            return registered;
        }

        /**
         * @param registered Sets registered flag.
         */
        void registered(boolean registered) {
            this.registered = registered;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /**
         * Sets type name.
         *
         * @param name Name.
         */
        void name(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Class<?>> fields() {
            return fields;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public <T> T value(String field, Object key, Object val) throws IgniteCheckedException {
            assert field != null;

            Property prop = props.get(field);

            if (prop == null)
                throw new IgniteCheckedException("Failed to find field '" + field + "' in type '" + name + "'.");

            return (T)prop.value(key, val);
        }

        /** {@inheritDoc} */
        @Override public Map<String, GridQueryIndexDescriptor> indexes() {
            return Collections.<String, GridQueryIndexDescriptor>unmodifiableMap(indexes);
        }

        /**
         * Adds index.
         *
         * @param idxName Index name.
         * @param type Index type.
         * @return Index descriptor.
         * @throws IgniteCheckedException In case of error.
         */
        public IndexDescriptor addIndex(String idxName, GridQueryIndexType type) throws IgniteCheckedException {
            IndexDescriptor idx = new IndexDescriptor(type);

            if (indexes.put(idxName, idx) != null)
                throw new IgniteCheckedException("Index with name '" + idxName + "' already exists.");

            return idx;
        }

        /**
         * Adds field to index.
         *
         * @param idxName Index name.
         * @param field Field name.
         * @param orderNum Fields order number in index.
         * @param descending Sorting order.
         * @throws IgniteCheckedException If failed.
         */
        public void addFieldToIndex(String idxName, String field, int orderNum,
            boolean descending) throws IgniteCheckedException {
            IndexDescriptor desc = indexes.get(idxName);

            if (desc == null)
                desc = addIndex(idxName, SORTED);

            desc.addField(field, orderNum, descending);
        }

        /**
         * Adds field to text index.
         *
         * @param field Field name.
         */
        public void addFieldToTextIndex(String field) {
            if (fullTextIdx == null) {
                fullTextIdx = new IndexDescriptor(FULLTEXT);

                indexes.put(null, fullTextIdx);
            }

            fullTextIdx.addField(field, 0, false);
        }

        /** {@inheritDoc} */
        @Override public Class<?> valueClass() {
            return valCls;
        }

        /**
         * Sets value class.
         *
         * @param valCls Value class.
         */
        void valueClass(Class<?> valCls) {
            this.valCls = valCls;
        }

        /** {@inheritDoc} */
        @Override public Class<?> keyClass() {
            return keyCls;
        }

        /**
         * Set key class.
         *
         * @param keyCls Key class.
         */
        void keyClass(Class<?> keyCls) {
            this.keyCls = keyCls;
        }

        /**
         * Adds property to the type descriptor.
         *
         * @param prop Property.
         * @param failOnDuplicate Fail on duplicate flag.
         * @throws IgniteCheckedException In case of error.
         */
        public void addProperty(Property prop, boolean failOnDuplicate) throws IgniteCheckedException {
            String name = prop.name();

            if (props.put(name, prop) != null && failOnDuplicate)
                throw new IgniteCheckedException("Property with name '" + name + "' already exists.");

            fields.put(name, prop.type());
        }

        /** {@inheritDoc} */
        @Override public boolean valueTextIndex() {
            return valTextIdx;
        }

        /**
         * Sets if this value should be text indexed.
         *
         * @param valTextIdx Flag value.
         */
        public void valueTextIndex(boolean valTextIdx) {
            this.valTextIdx = valTextIdx;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TypeDescriptor.class, this);
        }
    }

    /**
     * Index descriptor.
     */
    private static class IndexDescriptor implements GridQueryIndexDescriptor {
        /** Fields sorted by order number. */
        private final Collection<T2<String, Integer>> fields = new TreeSet<>(
            new Comparator<T2<String, Integer>>() {
                @Override public int compare(T2<String, Integer> o1, T2<String, Integer> o2) {
                    if (o1.get2().equals(o2.get2())) // Order is equal, compare field names to avoid replace in Set.
                        return o1.get1().compareTo(o2.get1());

                    return o1.get2() < o2.get2() ? -1 : 1;
                }
            });

        /** Fields which should be indexed in descending order. */
        private Collection<String> descendings;

        /** */
        private final GridQueryIndexType type;

        /**
         * @param type Type.
         */
        private IndexDescriptor(GridQueryIndexType type) {
            assert type != null;

            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> fields() {
            Collection<String> res = new ArrayList<>(fields.size());

            for (T2<String, Integer> t : fields)
                res.add(t.get1());

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean descending(String field) {
            return descendings != null && descendings.contains(field);
        }

        /**
         * Adds field to this index.
         *
         * @param field Field name.
         * @param orderNum Field order number in this index.
         * @param descending Sort order.
         */
        public void addField(String field, int orderNum, boolean descending) {
            fields.add(new T2<>(field, orderNum));

            if (descending) {
                if (descendings == null)
                    descendings  = new HashSet<>();

                descendings.add(field);
            }
        }

        /** {@inheritDoc} */
        @Override public GridQueryIndexType type() {
            return type;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexDescriptor.class, this);
        }
    }

    /**
     * Identifying TypeDescriptor by space and value class.
     */
    private static class TypeId {
        /** */
        private final String space;

        /** Value type. */
        private final Class<?> valType;

        /** Value type ID. */
        private final int valTypeId;

        /**
         * Constructor.
         *
         * @param space Space name.
         * @param valType Value type.
         */
        private TypeId(String space, Class<?> valType) {
            assert valType != null;

            this.space = space;
            this.valType = valType;

            valTypeId = 0;
        }

        /**
         * Constructor.
         *
         * @param space Space name.
         * @param valTypeId Value type ID.
         */
        private TypeId(String space, int valTypeId) {
            this.space = space;
            this.valTypeId = valTypeId;

            valType = null;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TypeId typeId = (TypeId)o;

            return (valTypeId == typeId.valTypeId) &&
                (valType != null ? valType == typeId.valType : typeId.valType == null) &&
                (space != null ? space.equals(typeId.space) : typeId.space == null);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 31 * (space != null ? space.hashCode() : 0) + (valType != null ? valType.hashCode() : valTypeId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TypeId.class, this);
        }
    }

    /**
     *
     */
    private static class TypeName {
        /** */
        private final String space;

        /** */
        private final String typeName;

        /**
         * @param space Space name.
         * @param typeName Type name.
         */
        private TypeName(@Nullable String space, String typeName) {
            assert !F.isEmpty(typeName) : typeName;

            this.space = space;
            this.typeName = typeName;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TypeName other = (TypeName)o;

            return (space != null ? space.equals(other.space) : other.space == null) &&
                typeName.equals(other.typeName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 31 * (space != null ? space.hashCode() : 0) + typeName.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TypeName.class, this);
        }
    }
}
