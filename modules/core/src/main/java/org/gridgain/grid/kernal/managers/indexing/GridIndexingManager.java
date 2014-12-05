/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.indexing;

import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.portables.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.spi.indexing.IndexType.*;

/**
 * Manages cache indexing.
 */
public class GridIndexingManager extends GridManagerAdapter<IndexingSpi> {
    /** */
    private IgniteMarshaller marsh;

    /** Type descriptors. */
    private final ConcurrentMap<TypeId, TypeDescriptor> types = new ConcurrentHashMap8<>();

    /** Type descriptors. */
    private final ConcurrentMap<TypeName, TypeDescriptor> typesByName = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<Long, ClassLoader> ldrById = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<ClassLoader, Long> idByLdr = new ConcurrentHashMap8<>();

    /** */
    private final AtomicLong ldrIdGen = new AtomicLong();

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Configuration-declared types. */
    private Map<TypeId, GridCacheQueryTypeMetadata> declaredTypesById;

    /** Configuration-declared types. */
    private final Map<TypeName, GridCacheQueryTypeMetadata> declaredTypesByName = new HashMap<>();

    /** Portable IDs. */
    private Map<Integer, String> portableIds;

    /** Type resolvers per space name. */
    private Map<String, GridCacheQueryTypeResolver> typeResolvers = new HashMap<>();

    /** */
    private ExecutorService execSvc;

    /**
     * @param ctx  Kernal context.
     */
    public GridIndexingManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getIndexingSpi());

        marsh = ctx.config().getMarshaller();
    }

    /**
     * @throws GridException Thrown in case of any errors.
     */
    @Override public void start() throws GridException {
        if (ctx.config().isDaemon())
            return;

        if (!enabled())
            U.warn(log, "Indexing is disabled (to enable please configure GridH2IndexingSpi).");

        IndexingMarshaller m = new IdxMarshaller();

        for (IndexingSpi spi : getSpis()) {
            spi.registerMarshaller(m);

            for (GridCacheConfiguration cacheCfg : ctx.config().getCacheConfiguration())
                spi.registerSpace(cacheCfg.getName());
        }

        execSvc = ctx.config().getExecutorService();

        startSpi();

        for (GridCacheConfiguration ccfg : ctx.config().getCacheConfiguration()){
            GridCacheQueryConfiguration qryCfg = ccfg.getQueryConfiguration();

            if (qryCfg != null) {
                for (GridCacheQueryTypeMetadata meta : qryCfg.getTypeMetadata())
                    declaredTypesByName.put(new TypeName(ccfg.getName(), meta.getType()), meta);

                if (qryCfg.getTypeResolver() != null)
                    typeResolvers.put(ccfg.getName(), qryCfg.getTypeResolver());
            }
        }

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        if (ctx.config().isDaemon())
            return;

        busyLock.block();
    }

    /**
     * @throws GridException Thrown in case of any errors.
     */
    @Override public void stop(boolean cancel) throws GridException {
        if (ctx.config().isDaemon())
            return;

        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Returns number of objects of given type for given space of spi.
     *
     * @param spi SPI Name.
     * @param space Space.
     * @param valType Value type.
     * @return Objects number or -1 if this type is unknown for given SPI and space.
     * @throws GridException If failed.
     */
    public long size(@Nullable String spi, @Nullable String space, Class<?> valType) throws GridException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to get space size (grid is stopping).");

        try {
            TypeDescriptor desc = types.get(new TypeId(space, valType));

            if (desc == null || !desc.registered())
                return -1;

            return getSpi(spi).size(space, desc);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Rebuilds all search indexes of given value type for given space of spi.
     *
     * @param spi SPI name.
     * @param space Space.
     * @param valTypeName Value type name.
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    public IgniteFuture<?> rebuildIndexes(@Nullable final String spi, @Nullable final String space, String valTypeName) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to rebuild indexes (grid is stopping).");

        try {
            return rebuildIndexes(spi, space, typesByName.get(new TypeName(space, valTypeName)));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param spi SPI name.
     * @param space Space.
     * @param desc Type descriptor.
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    private IgniteFuture<?> rebuildIndexes(@Nullable final String spi, @Nullable final String space,
        @Nullable final TypeDescriptor desc) {
        if (desc == null || !desc.registered())
            return new GridFinishedFuture<Void>(ctx);

        final GridWorkerFuture<?> fut = new GridWorkerFuture<Void>();

        GridWorker w = new GridWorker(ctx.gridName(), "index-rebuild-worker", log) {
            @Override protected void body() {
                try {
                    getSpi(spi).rebuildIndexes(space, desc);

                    fut.onDone();
                }
                catch (Exception e) {
                    fut.onDone(e);
                }
                catch (Throwable e) {
                    log.error("Failed to rebuild indexes for type: " + desc.name(), e);

                    fut.onDone(e);
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
     * @param spi SPI name.
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    @SuppressWarnings("unchecked")
    public IgniteFuture<?> rebuildAllIndexes(@Nullable String spi) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to get space size (grid is stopping).");

        try {
            GridCompoundFuture<?, ?> fut = new GridCompoundFuture<Object, Object>(ctx);

            for (Map.Entry<TypeId, TypeDescriptor> e : types.entrySet())
                fut.add((IgniteFuture)rebuildIndexes(spi, e.getKey().space, e.getValue()));

            fut.markInitialized();

            return fut;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * FOR TESTING ONLY
     *
     * @param name SPI Name.
     * @return SPI.
     */
    public IndexingSpi spi(@Nullable String name) {
        if (F.isEmpty(name))
            return getSpis()[0];

        for (IndexingSpi s : getSpis()) {
            if (name.equals(s.getName()))
                return s;
        }

        throw new GridRuntimeException("Failed to find SPI for name: " + name);
    }

    /**
     * @param x Value.
     * @param bytes Serialized value.
     * @param <T> Value type.
     * @return Index entry.
     */
    private <T> IndexingEntity<T> entry(T x, @Nullable byte[] bytes) {
        return new IndexingEntityAdapter<>(x, bytes);
    }

    /**
     * Writes key-value pair to index.
     *
     * @param spi SPI Name.
     * @param space Space.
     * @param key Key.
     * @param keyBytes Byte array with key data.
     * @param val Value.
     * @param valBytes Byte array with value data.
     * @param ver Cache entry version.
     * @param expirationTime Expiration time or 0 if never expires.
     * @throws GridException In case of error.
     */
    @SuppressWarnings("unchecked")
    public <K, V> void store(final String spi, final String space, final K key, @Nullable byte[] keyBytes, final V val,
        @Nullable byte[] valBytes, byte[] ver, long expirationTime) throws GridException {
        assert key != null;
        assert val != null;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to write to index (grid is stopping).");

        try {
            if (log.isDebugEnabled())
                log.debug("Storing key to cache query index [key=" + key + ", value=" + val + "]");

            final Class<?> valCls = val.getClass();
            final Class<?> keyCls = key.getClass();

            TypeId id = null;

            GridCacheQueryTypeResolver rslvr = typeResolvers.get(space);

            if (rslvr != null) {
                String typeName = rslvr.resolveTypeName(key, val);

                if (typeName != null)
                    id = new TypeId(space, ctx.portable().typeId(typeName));
            }

            if (id == null) {
                if (val instanceof PortableObject) {
                    PortableObject portable = (PortableObject)val;

                    int typeId = portable.typeId();

                    String typeName = portableName(typeId);

                    if (typeName == null)
                        return;

                    id = new TypeId(space, typeId);
                }
                else
                    id = new TypeId(space, valCls);
            }

            TypeDescriptor desc = types.get(id);

            if (desc == null) {
                desc = new TypeDescriptor();

                TypeDescriptor existing = types.putIfAbsent(id, desc);

                if (existing != null)
                    desc = existing;
            }

            if (!desc.succeeded()) {
                final TypeDescriptor d = desc;

                d.init(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        d.keyClass(keyCls);
                        d.valueClass(valCls);

                        if (key instanceof PortableObject) {
                            PortableObject portableKey = (PortableObject)key;

                            String typeName = portableName(portableKey.typeId());

                            if (typeName != null) {
                                GridCacheQueryTypeMetadata keyMeta = declaredType(space, portableKey.typeId());

                                if (keyMeta != null)
                                    processPortableMeta(true, keyMeta, d);
                            }
                        }
                        else {
                            GridCacheQueryTypeMetadata keyMeta = declaredType(space, keyCls.getName());

                            if (keyMeta == null)
                                processAnnotationsInClass(true, d.keyCls, d, null);
                            else
                                processClassMeta(true, d.keyCls, keyMeta, d);
                        }

                        if (val instanceof PortableObject) {
                            PortableObject portableVal = (PortableObject)val;

                            String typeName = portableName(portableVal.typeId());

                            if (typeName != null) {
                                GridCacheQueryTypeMetadata valMeta = declaredType(space, portableVal.typeId());

                                d.name(typeName);

                                if (valMeta != null)
                                    processPortableMeta(false, valMeta, d);
                            }
                        }
                        else {
                            String valTypeName = typeName(valCls);

                            d.name(valTypeName);

                            GridCacheQueryTypeMetadata typeMeta = declaredType(space, valCls.getName());

                            if (typeMeta == null)
                                processAnnotationsInClass(false, d.valCls, d, null);
                            else
                                processClassMeta(false, d.valCls, typeMeta, d);
                        }

                        d.registered(getSpi(spi).registerType(space, d));

                        typesByName.put(new TypeName(space, d.name()), d);

                        return null;
                    }
                });
            }

            if (!desc.registered())
                return;

            if (!desc.valueClass().equals(valCls))
                throw new GridException("Failed to update index due to class name conflict" +
                    "(multiple classes with same simple name are stored in the same cache) " +
                    "[expCls=" + desc.valueClass().getName() + ", actualCls=" + valCls.getName() + ']');

            IndexingEntity<K> k = entry(key, keyBytes);
            IndexingEntity<V> v = entry(val, valBytes);

            getSpi(spi).store(space, desc, k, v, ver, expirationTime);
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
    public String typeName(Class<?> cls) {
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
     * Gets portable type name by portable ID.
     *
     * @param typeId Type ID.
     * @return Name.
     */
    private String portableName(int typeId) {
        Map<Integer, String> portableIds = this.portableIds;

        if (portableIds == null) {
            portableIds = new HashMap<>();

            for (GridCacheConfiguration ccfg : ctx.config().getCacheConfiguration()){
                GridCacheQueryConfiguration qryCfg = ccfg.getQueryConfiguration();

                if (qryCfg != null) {
                    for (GridCacheQueryTypeMetadata meta : qryCfg.getTypeMetadata())
                        portableIds.put(ctx.portable().typeId(meta.getType()), meta.getType());
                }
            }

            this.portableIds = portableIds;
        }

        return portableIds.get(typeId);
    }

    /**
     * @param space Space name.
     * @param typeId Type ID.
     * @return Type meta data if it was declared in configuration.
     */
    @Nullable private GridCacheQueryTypeMetadata declaredType(String space, int typeId) {
        Map<TypeId, GridCacheQueryTypeMetadata> declaredTypesById = this.declaredTypesById;

        if (declaredTypesById == null) {
            declaredTypesById = new HashMap<>();

            for (GridCacheConfiguration ccfg : ctx.config().getCacheConfiguration()){
                GridCacheQueryConfiguration qryCfg = ccfg.getQueryConfiguration();

                if (qryCfg != null) {
                    for (GridCacheQueryTypeMetadata meta : qryCfg.getTypeMetadata())
                        declaredTypesById.put(new TypeId(ccfg.getName(), ctx.portable().typeId(meta.getType())), meta);
                }
            }

            this.declaredTypesById = declaredTypesById;
        }

        return declaredTypesById.get(new TypeId(space, typeId));
    }

    /**
     * @param space Space name.
     * @param typeName Type name.
     * @return Type meta data if it was declared in configuration.
     */
    @Nullable private GridCacheQueryTypeMetadata declaredType(String space, String typeName) {
        return declaredTypesByName.get(new TypeName(space, typeName));
    }

    /**
     * @param spi SPI Name.
     * @param space Space.
     * @param key Key.
     * @param keyBytes Byte array with key value.
     * @return {@code true} if key was found and removed, otherwise {@code false}.
     * @throws GridException Thrown in case of any errors.
     */
    @SuppressWarnings("unchecked")
    public <K> boolean remove(String spi, String space, K key, @Nullable byte[] keyBytes) throws GridException {
        assert key != null;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to remove from index (grid is stopping).");

        try {
            IndexingEntity<K> k = entry(key, keyBytes);

            return getSpi(spi).remove(space, k);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param spi SPI Name.
     * @param space Space name.
     * @param clause Clause.
     * @param params Parameters collection.
     * @param includeBackups Include or exclude backup entries.
     * @param filters Key and value filters.
     * @return Field rows.
     * @throws GridException If failed.
     */
    public <K, V> IndexingFieldsResult queryFields(@Nullable String spi, @Nullable String space,
        String clause, Collection<Object> params, boolean includeBackups,
        IndexingQueryFilter filters) throws GridException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            IndexingQueryFilter backupFilter = backupsFilter(includeBackups);

            return getSpi(spi).queryFields(space, clause, params,
                and(filters, backupFilter));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param f1 First filter.
     * @param f2 Second filter.
     * @return And filter of the given two.
     */
    @Nullable private static IndexingQueryFilter and(@Nullable final IndexingQueryFilter f1,
        @Nullable final IndexingQueryFilter f2) {
        if (f1 == null)
            return f2;

        if (f2 == null)
            return f1;

        return new IndexingQueryFilter() {
            @Nullable @Override public <K, V> IgniteBiPredicate<K, V> forSpace(String spaceName) throws GridException {
                final IgniteBiPredicate<K, V> fltr1 = f1.forSpace(spaceName);
                final IgniteBiPredicate<K, V> fltr2 = f2.forSpace(spaceName);

                if (fltr1 == null)
                    return fltr2;

                if (fltr2 == null)
                    return fltr1;

                return new IgniteBiPredicate<K, V>() {
                    @Override public boolean apply(K k, V v) {
                        return fltr1.apply(k, v) && fltr2.apply(k, v);
                    }
                };
            }
        };
    }

    /**
     * @param spi SPI Name.
     * @param space Space.
     * @param clause Clause.
     * @param params Parameters collection.
     * @param resType Result type.
     * @param includeBackups Include or exclude backup entries.
     * @param filters Filters.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Key/value rows.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCloseableIterator<IndexingKeyValueRow<K, V>> query(String spi, String space, String clause,
        Collection<Object> params, String resType, boolean includeBackups,
        IndexingQueryFilter filters) throws GridException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            TypeDescriptor type = typesByName.get(new TypeName(space, resType));

            if (type == null || !type.registered())
                return new GridEmptyCloseableIterator<>();

            IndexingQueryFilter backupFilter = backupsFilter(includeBackups);

            return new GridSpiCloseableIteratorWrapper<>(getSpi(spi).<K,V>query(space, clause, params, type,
                and(filters, backupFilter)));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param spi SPI Name.
     * @param space Space.
     * @param clause Clause.
     * @param resType Result type.
     * @param includeBackups Include or exclude backup entries.
     * @param filters Key and value filters.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Key/value rows.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCloseableIterator<IndexingKeyValueRow<K, V>> queryText(String spi, String space,
        String clause, String resType, boolean includeBackups,
        IndexingQueryFilter filters) throws GridException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            TypeDescriptor type = typesByName.get(new TypeName(space, resType));

            if (type == null || !type.registered())
                return new GridEmptyCloseableIterator<>();

            IndexingQueryFilter backupFilter = backupsFilter(includeBackups);

            return new GridSpiCloseableIteratorWrapper<>(getSpi(spi).<K,V>queryText(space, clause, type,
                and(filters, backupFilter)));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Predicate.
     * @param includeBackups Include backups.
     */
    @SuppressWarnings("unchecked")
    @Nullable private <K, V> IndexingQueryFilter backupsFilter(boolean includeBackups) {
        if (includeBackups)
            return null;

        return new IndexingQueryFilter() {
            @Nullable @Override public IgniteBiPredicate<K, V> forSpace(final String spaceName) {
                final GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(spaceName);

                if (cache.context().isReplicated() || cache.configuration().getBackups() == 0)
                    return null;

                return new IgniteBiPredicate<K, V>() {
                    @Override public boolean apply(K k, V v) {
                        return cache.context().affinity().primary(ctx.discovery().localNode(), k, -1);
                    }
                };
            }
        };
    }

    /**
     * Will be called when entry for key will be swapped.
     *
     * @param spi Spi name.
     * @param spaceName Space name.
     * @param swapSpaceName Swap space name.
     * @param key key.
     * @throws org.apache.ignite.spi.IgniteSpiException If failed.
     */
    public void onSwap(String spi, String spaceName, String swapSpaceName, Object key) throws IgniteSpiException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process swap event (grid is stopping).");

        try {
            getSpi(spi).onSwap(spaceName, swapSpaceName, key);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Will be called when entry for key will be unswapped.
     *
     * @param spi Spi name.
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @param valBytes Value bytes.
     * @throws org.apache.ignite.spi.IgniteSpiException If failed.
     */
    public void onUnswap(String spi, String spaceName, Object key, Object val, byte[] valBytes)
        throws IgniteSpiException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process swap event (grid is stopping).");

        try {
            getSpi(spi).onUnswap(spaceName, key, val, valBytes);
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
     * @throws GridException If undeploy failed.
     */
    public void onUndeploy(@Nullable String space, ClassLoader ldr) throws GridException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process undeploy event (grid is stopping).");

        try {
            try {
                Iterator<Map.Entry<TypeId, TypeDescriptor>> it = types.entrySet().iterator();

                while (it.hasNext()) {
                    Map.Entry<TypeId, TypeDescriptor> e = it.next();

                    if (!F.eq(e.getKey().space, space))
                        continue;

                    TypeDescriptor desc = e.getValue();

                    if (ldr.equals(U.detectClassLoader(desc.valCls)) || ldr.equals(U.detectClassLoader(desc.keyCls))) {
                        for (IndexingSpi spi : getSpis()) {
                            if (desc.await() && desc.registered())
                                spi.unregisterType(e.getKey().space, desc);
                        }

                        it.remove();
                    }
                }
            }
            finally {
                Long id = idByLdr.remove(ldr);

                if (id != null)
                    ldrById.remove(id);
            }
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Process annotations for class.
     *
     * @param key If given class relates to key.
     * @param cls Class.
     * @param type Type descriptor.
     * @param parent Parent in case of embeddable.
     * @throws GridException In case of error.
     */
    static void processAnnotationsInClass(boolean key, Class<?> cls, TypeDescriptor type,
        @Nullable ClassProperty parent) throws GridException {
        if (U.isJdk(cls))
            return;

        if (parent != null && parent.knowsClass(cls))
            throw new GridException("Recursive reference found in type: " + cls.getName());

        if (parent == null) { // Check class annotation at top level only.
            GridCacheQueryTextField txtAnnCls = cls.getAnnotation(GridCacheQueryTextField.class);

            if (txtAnnCls != null)
                type.valueTextIndex(true);

            GridCacheQueryGroupIndex grpIdx = cls.getAnnotation(GridCacheQueryGroupIndex.class);

            if (grpIdx != null)
                type.addIndex(grpIdx.name(), SORTED);

            GridCacheQueryGroupIndex.List grpIdxList = cls.getAnnotation(GridCacheQueryGroupIndex.List.class);

            if (grpIdxList != null && !F.isEmpty(grpIdxList.value())) {
                for (GridCacheQueryGroupIndex idx : grpIdxList.value())
                    type.addIndex(idx.name(), SORTED);
            }
        }

        for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                GridCacheQuerySqlField sqlAnn = field.getAnnotation(GridCacheQuerySqlField.class);
                GridCacheQueryTextField txtAnn = field.getAnnotation(GridCacheQueryTextField.class);

                if (sqlAnn != null || txtAnn != null) {
                    ClassProperty prop = new ClassProperty(field);

                    prop.parent(parent);

                    processAnnotation(key, sqlAnn, txtAnn, field.getType(), prop, type);

                    type.addProperty(key, prop, true);
                }
            }

            for (Method mtd : c.getDeclaredMethods()) {
                GridCacheQuerySqlField sqlAnn = mtd.getAnnotation(GridCacheQuerySqlField.class);
                GridCacheQueryTextField txtAnn = mtd.getAnnotation(GridCacheQueryTextField.class);

                if (sqlAnn != null || txtAnn != null) {
                    if (mtd.getParameterTypes().length != 0)
                        throw new GridException("Getter with GridCacheQuerySqlField " +
                            "annotation cannot have parameters: " + mtd);

                    ClassProperty prop = new ClassProperty(mtd);

                    prop.parent(parent);

                    processAnnotation(key, sqlAnn, txtAnn, mtd.getReturnType(), prop, type);

                    type.addProperty(key, prop, true);
                }
            }
        }
    }

    /**
     * Processes annotation at field or method.
     *
     * @param key If given class relates to key.
     * @param sqlAnn SQL annotation, can be {@code null}.
     * @param txtAnn H2 text annotation, can be {@code null}.
     * @param cls Class of field or return type for method.
     * @param prop Current property.
     * @param desc Class description.
     * @throws GridException In case of error.
     */
    static void processAnnotation(boolean key, GridCacheQuerySqlField sqlAnn, GridCacheQueryTextField txtAnn,
        Class<?> cls, ClassProperty prop, TypeDescriptor desc) throws GridException {
        if (sqlAnn != null) {
            processAnnotationsInClass(key, cls, desc, prop);

            if (!sqlAnn.name().isEmpty())
                prop.name(sqlAnn.name());

            if (sqlAnn.index() || sqlAnn.unique()) {
                String idxName = prop.name() + "_idx";

                desc.addIndex(idxName, isGeometryClass(prop.type()) ? GEO_SPATIAL : SORTED);

                desc.addFieldToIndex(idxName, prop.name(), 0, sqlAnn.descending());
            }

            if (!F.isEmpty(sqlAnn.groups())) {
                for (String group : sqlAnn.groups())
                    desc.addFieldToIndex(group, prop.name(), 0, false);
            }

            if (!F.isEmpty(sqlAnn.orderedGroups())) {
                for (GridCacheQuerySqlField.Group idx : sqlAnn.orderedGroups())
                    desc.addFieldToIndex(idx.name(), prop.name(), idx.order(), idx.descending());
            }
        }

        if (txtAnn != null)
            desc.addFieldToTextIndex(prop.name());
    }

    /**
     * Processes declarative metadata for class.
     *
     * @param key Key or value flag.
     * @param cls Class to process.
     * @param meta Type metadata.
     * @param d Type descriptor.
     * @throws GridException If failed.
     */
    static void processClassMeta(boolean key, Class<?> cls, GridCacheQueryTypeMetadata meta, TypeDescriptor d)
        throws GridException {
        for (Map.Entry<String, Class<?>> entry : meta.getAscendingFields().entrySet()) {
            ClassProperty prop = buildClassProperty(cls, entry.getKey(), entry.getValue());

            d.addProperty(key, prop, false);

            String idxName = prop.name() + "_idx";

            d.addIndex(idxName, isGeometryClass(prop.type()) ? GEO_SPATIAL : SORTED);

            d.addFieldToIndex(idxName, prop.name(), 0, false);
        }

        for (Map.Entry<String, Class<?>> entry : meta.getDescendingFields().entrySet()) {
            ClassProperty prop = buildClassProperty(cls, entry.getKey(), entry.getValue());

            d.addProperty(key, prop, false);

            String idxName = prop.name() + "_idx";

            d.addIndex(idxName, isGeometryClass(prop.type()) ? GEO_SPATIAL : SORTED);

            d.addFieldToIndex(idxName, prop.name(), 0, true);
        }

        for (String txtIdx : meta.getTextFields()) {
            ClassProperty prop = buildClassProperty(cls, txtIdx, String.class);

            d.addProperty(key, prop, false);

            d.addFieldToTextIndex(prop.name());
        }

        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> grps = meta.getGroups();

        if (grps != null) {
            for (Map.Entry<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> entry : grps.entrySet()) {
                String idxName = entry.getKey();

                LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> idxFields = entry.getValue();

                int order = 0;

                for (Map.Entry<String, IgniteBiTuple<Class<?>, Boolean>> idxField : idxFields.entrySet()) {
                    ClassProperty prop = buildClassProperty(cls, idxField.getKey(), idxField.getValue().get1());

                    d.addProperty(key, prop, false);

                    Boolean descending = idxField.getValue().get2();

                    d.addFieldToIndex(idxName, prop.name(), order, descending != null && descending);

                    order++;
                }
            }
        }

        for (Map.Entry<String, Class<?>> entry : meta.getQueryFields().entrySet()) {
            ClassProperty prop = buildClassProperty(cls, entry.getKey(), entry.getValue());

            d.addProperty(key, prop, false);
        }
    }

    /**
     * Processes declarative metadata for portable object.
     *
     * @param key Key or value flag.
     * @param meta Declared metadata.
     * @param d Type descriptor.
     * @throws GridException If failed.
     */
    static void processPortableMeta(boolean key, GridCacheQueryTypeMetadata meta, TypeDescriptor d)
        throws GridException {
        for (Map.Entry<String, Class<?>> entry : meta.getAscendingFields().entrySet()) {
            PortableProperty prop = buildPortableProperty(entry.getKey(), entry.getValue());

            d.addProperty(key, prop, false);

            String idxName = prop.name() + "_idx";

            d.addIndex(idxName, isGeometryClass(prop.type()) ? GEO_SPATIAL : SORTED);

            d.addFieldToIndex(idxName, prop.name(), 0, false);
        }

        for (Map.Entry<String, Class<?>> entry : meta.getDescendingFields().entrySet()) {
            PortableProperty prop = buildPortableProperty(entry.getKey(), entry.getValue());

            d.addProperty(key, prop, false);

            String idxName = prop.name() + "_idx";

            d.addIndex(idxName, isGeometryClass(prop.type()) ? GEO_SPATIAL : SORTED);

            d.addFieldToIndex(idxName, prop.name(), 0, true);
        }

        for (String txtIdx : meta.getTextFields()) {
            PortableProperty prop = buildPortableProperty(txtIdx, String.class);

            d.addProperty(key, prop, false);

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

                    d.addProperty(key, prop, false);

                    Boolean descending = idxField.getValue().get2();

                    d.addFieldToIndex(idxName, prop.name(), order, descending != null && descending);

                    order++;
                }
            }
        }

        for (Map.Entry<String, Class<?>> entry : meta.getQueryFields().entrySet()) {
            PortableProperty prop = buildPortableProperty(entry.getKey(), entry.getValue());

            if (!d.props.containsKey(prop.name()))
                d.addProperty(key, prop, false);
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
    static PortableProperty buildPortableProperty(String pathStr, Class<?> resType) {
        String[] path = pathStr.split("\\.");

        PortableProperty res = null;

        for (String prop : path)
            res = new PortableProperty(prop, res, resType);

        return res;
    }

    /**
     * @param cls Source type class.
     * @param pathStr String representing path to the property. May contains dots '.' to identify nested fields.
     * @param resType Expected result type.
     * @return Property instance corresponding to the given path.
     * @throws GridException If property cannot be created.
     */
    static ClassProperty buildClassProperty(Class<?> cls, String pathStr, Class<?> resType) throws GridException {
        String[] path = pathStr.split("\\.");

        ClassProperty res = null;

        for (String prop : path) {
            ClassProperty tmp;

            try {
                StringBuilder bld = new StringBuilder("get");

                bld.append(prop);

                bld.setCharAt(3, Character.toUpperCase(bld.charAt(3)));

                tmp = new ClassProperty(cls.getMethod(bld.toString()));
            }
            catch (NoSuchMethodException ignore) {
                try {
                    tmp = new ClassProperty(cls.getDeclaredField(prop));
                }
                catch (NoSuchFieldException ignored) {
                    throw new GridException("Failed to find getter method or field for property named " +
                        "'" + prop + "': " + cls.getName());
                }
            }

            tmp.parent(res);

            cls = tmp.type();

            res = tmp;
        }

        if (!U.box(resType).isAssignableFrom(U.box(res.type())))
            throw new GridException("Failed to create property for given path (actual property type is not assignable" +
                " to declared type [path=" + pathStr + ", actualType=" + res.type().getName() +
                ", declaredType=" + resType.getName() + ']');

        return res;
    }

    /**
     * Gets types for space.
     *
     * @param space Space name.
     * @return Descriptors.
     */
    public Collection<IndexingTypeDescriptor> types(@Nullable String space) {
        Collection<IndexingTypeDescriptor> spaceTypes = new ArrayList<>(
            Math.min(10, types.size()));

        for (Map.Entry<TypeId, TypeDescriptor> e : types.entrySet()) {
            TypeDescriptor desc = e.getValue();

            if (desc.registered() && F.eq(e.getKey().space, space))
                spaceTypes.add(desc);
        }

        return spaceTypes;
    }

    /**
     * Gets type for space and type name.
     *
     * @param space Space name.
     * @param typeName Type name.
     * @return Type.
     * @throws GridException If failed.
     */
    public IndexingTypeDescriptor type(@Nullable String space, String typeName) throws GridException {
        TypeDescriptor type = typesByName.get(new TypeName(space, typeName));

        if (type == null || !type.registered())
            throw new GridException("Failed to find type descriptor for type name: " + typeName);

        return type;
    }

    /**
     * @param cls Field type.
     * @return {@code True} if given type is a spatial geometry type based on {@code com.vividsolutions.jts} library.
     * @throws GridException If failed.
     */
    private static boolean isGeometryClass(Class<?> cls) throws GridException {
        Class<?> dataTypeCls;

        try {
            dataTypeCls = Class.forName("org.h2.value.DataType");
        }
        catch (ClassNotFoundException ignored) {
            return false; // H2 is not in classpath.
        }

        try {
            Method method = dataTypeCls.getMethod("isGeometryClass", Class.class);

            return (Boolean)method.invoke(null, cls);
        }
        catch (Exception e) {
            throw new GridException("Failed to invoke 'org.h2.value.DataType.isGeometryClass' method.", e);
        }
    }

    /**
     *
     */
    private abstract static class Property {
        /**
         * Gets this property value from the given object.
         *
         * @param x Object with this property.
         * @return Property value.
         * @throws org.apache.ignite.spi.IgniteSpiException If failed.
         */
        public abstract Object value(Object x) throws IgniteSpiException;

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

        /**
         * Constructor.
         *
         * @param member Element.
         */
        ClassProperty(Member member) {
            this.member = member;

            name = member instanceof Method && member.getName().startsWith("get") && member.getName().length() > 3 ?
                member.getName().substring(3) : member.getName();

            ((AccessibleObject) member).setAccessible(true);

            field = member instanceof Field;
        }

        /** {@inheritDoc} */
        @Override public Object value(Object x) throws IgniteSpiException {
            if (parent != null)
                x = parent.value(x);

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
                throw new IgniteSpiException(e);
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
    private static class PortableProperty extends Property {
        /** Property name. */
        private String propName;

        /** Parent property. */
        private PortableProperty parent;

        /** Result class. */
        private Class<?> type;

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
        @Override public Object value(Object obj) throws IgniteSpiException {
            if (parent != null)
                obj = parent.value(obj);

            if (obj == null)
                return null;

            if (!(obj instanceof PortableObject))
                throw new IgniteSpiException("Non-portable object received as a result of property extraction " +
                    "[parent=" + parent + ", propName=" + propName + ", obj=" + obj + ']');

            try {
                return ((PortableObject)obj).field(propName);
            }
            catch (PortableException e) {
                throw new IgniteSpiException(e);
            }
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
    private static class TypeDescriptor implements IndexingTypeDescriptor {
        /** */
        private String name;

        /** Value field names and types with preserved order. */
        @GridToStringInclude
        private final Map<String, Class<?>> valFields = new LinkedHashMap<>();

        /** */
        @GridToStringExclude
        private final Map<String, Property> props = new HashMap<>();

        /** Key field names and types with preserved order. */
        @GridToStringInclude
        private final Map<String, Class<?>> keyFields = new LinkedHashMap<>();

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

        /** To ensure that type was registered in SPI and only once. */
        private final GridAtomicInitializer<Void> initializer = new GridAtomicInitializer<>();

        /** SPI can decide not to register this type. */
        private boolean registered;

        /**
         * @param c Initialization callable.
         * @throws GridException In case of error.
         */
        void init(Callable<Void> c) throws GridException {
            initializer.init(c);
        }

        /**
         * @return Waits for initialization.
         * @throws GridInterruptedException If thread is interrupted.
         */
        boolean await() throws GridInterruptedException {
            return initializer.await();
        }

        /**
         * @return Whether initialization was successfully completed.
         */
        boolean succeeded() {
            return initializer.succeeded();
        }

        /**
         * @return {@code True} if type registration in SPI was finished and type was not rejected.
         */
        boolean registered() {
            return initializer.succeeded() && registered;
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
        @Override public Map<String, Class<?>> valueFields() {
            return valFields;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Class<?>> keyFields() {
            return keyFields;
        }

        /** {@inheritDoc} */
        @Override public <T> T value(Object obj, String field) throws IgniteSpiException {
            assert obj != null;
            assert field != null;

            Property prop = props.get(field);

            if (prop == null)
                throw new IgniteSpiException("Failed to find field '" + field + "' in type '" + name + "'.");

            return (T)prop.value(obj);
        }

        /** {@inheritDoc} */
        @Override public Map<String, org.gridgain.grid.spi.indexing.IndexDescriptor> indexes() {
            return Collections.<String, org.gridgain.grid.spi.indexing.IndexDescriptor>unmodifiableMap(indexes);
        }

        /**
         * Adds index.
         *
         * @param idxName Index name.
         * @param type Index type.
         * @return Index descriptor.
         * @throws GridException In case of error.
         */
        public IndexDescriptor addIndex(String idxName, IndexType type) throws GridException {
            IndexDescriptor idx = new IndexDescriptor(type);

            if (indexes.put(idxName, idx) != null)
                throw new GridException("Index with name '" + idxName + "' already exists.");

            return idx;
        }

        /**
         * Adds field to index.
         *
         * @param idxName Index name.
         * @param field Field name.
         * @param orderNum Fields order number in index.
         * @param descending Sorting order.
         * @throws GridException If failed.
         */
        public void addFieldToIndex(String idxName, String field, int orderNum,
            boolean descending) throws GridException {
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
         * @param key If given property relates to key.
         * @param prop Property.
         * @param failOnDuplicate Fail on duplicate flag.
         * @throws GridException In case of error.
         */
        public void addProperty(boolean key, Property prop, boolean failOnDuplicate) throws GridException {
            String name = prop.name();

            if (props.put(name, prop) != null && failOnDuplicate)
                throw new GridException("Property with name '" + name + "' already exists.");

            if (key)
                keyFields.put(name, prop.type());
            else
                valFields.put(name, prop.type());
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
    private static class IndexDescriptor implements org.gridgain.grid.spi.indexing.IndexDescriptor {
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
        private final IndexType type;

        /**
         * @param type Type.
         */
        private IndexDescriptor(IndexType type) {
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
        @Override public IndexType type() {
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

    /**
     * Indexing marshaller which also stores information about class loader and allows lazy unmarshalling.
     */
    private class IdxMarshaller implements IndexingMarshaller {
        /** {@inheritDoc} */
        @Override public <T> IndexingEntity<T> unmarshal(final byte[] bytes) {
            long ldrId = bytes[0] == -1 ? 0 : U.bytesToLong(bytes, 0);

            final ClassLoader ldr = ldrId == 0 ? null : ldrById.get(ldrId);

            final int off = ldrId == 0 ? 1 : 8;

            final int len = bytes.length - off;

            return new IndexingEntity<T>() {
                /** */
                private T val;

                /** */
                private byte[] valBytes;

                @Override public T value() throws IgniteSpiException {
                    if (val == null) {
                        try {
                            val = marsh.unmarshal(new ByteArrayInputStream(bytes, off, len), ldr);
                        }
                        catch (GridException e) {
                            throw new IgniteSpiException(e);
                        }
                    }

                    return val;
                }

                @Override public byte[] bytes() {
                    if (valBytes == null) {
                        byte[] bs = new byte[len];

                        U.arrayCopy(bytes, off, bs, 0, len);

                        valBytes = bs;
                    }

                    return valBytes;
                }

                @Override public boolean hasValue() {
                    return val != null;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public byte[] marshal(IndexingEntity<?> entity) throws IgniteSpiException {
            Object val = entity.value();

            ClassLoader ldr = val.getClass().getClassLoader();

            byte[] bytes = entity.bytes();

            ByteArrayOutputStream out = new ByteArrayOutputStream(bytes == null ? 128 : bytes.length + 8);

            if (ldr == null)
                // In special case of bootstrap class loader ldrId will be one byte -1.
                out.write(-1);
            else {
                Long ldrId;

                while ((ldrId = idByLdr.get(ldr)) == null) {
                    ldrId = ldrIdGen.incrementAndGet();

                    if (idByLdr.putIfAbsent(ldr, ldrId) == null) {
                        ldrById.put(ldrId, ldr);

                        break;
                    }
                }

                try {
                    out.write(U.longToBytes(ldrId));
                }
                catch (IOException e) {
                    throw new IgniteSpiException(e);
                }
            }

            try {
                if (bytes == null)
                    marsh.marshal(val, out);
                else
                    out.write(bytes);
            }
            catch (Exception e) {
                throw new IgniteSpiException(e);
            }

            return out.toByteArray();
        }
    }
}
