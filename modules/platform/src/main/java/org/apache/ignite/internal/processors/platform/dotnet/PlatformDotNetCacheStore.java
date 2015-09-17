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

package org.apache.ignite.internal.processors.platform.dotnet;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.portable.PortableRawReaderEx;
import org.apache.ignite.internal.portable.PortableRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.cache.store.PlatformCacheStore;
import org.apache.ignite.internal.processors.platform.cache.store.PlatformCacheStoreCallback;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper for .NET cache store implementations.
 * <p>
 * This wrapper should be used if you have an implementation of
 * {@code GridGain.Cache.IGridCacheStore} interface in .NET and
 * would like to configure it a persistence storage for your cache.
 * If properly configured, this wrapper will instantiate an instance
 * of your cache store in .NET and delegate all calls to that instance.
 * To create an instance, assembly name and class name are passed to
 * <a target="_blank" href="http://msdn.microsoft.com/en-us/library/d133hta4.aspx">System.Activator.CreateInstance(String, String)</a>
 * method in .NET during node startup. Refer to its documentation for
 * details.
 */
public class PlatformDotNetCacheStore<K, V> implements CacheStore<K, V>, PlatformCacheStore {
    /** Load cache operation code. */
    private static final byte OP_LOAD_CACHE = (byte)0;

    /** Load operation code. */
    private static final byte OP_LOAD = (byte)1;

    /** Load all operation code. */
    private static final byte OP_LOAD_ALL = (byte)2;

    /** Put operation code. */
    private static final byte OP_PUT = (byte)3;

    /** Put all operation code. */
    private static final byte OP_PUT_ALL = (byte)4;

    /** Remove operation code. */
    private static final byte OP_RMV = (byte)5;

    /** Remove all operation code. */
    private static final byte OP_RMV_ALL = (byte)6;

    /** Tx end operation code. */
    private static final byte OP_SES_END = (byte)7;

    /** Key used to distinguish session deployment.  */
    private static final Object KEY_SES = new Object();

    /** */
    @CacheStoreSessionResource
    private CacheStoreSession ses;

    /** .Net assembly name. */
    private String assemblyName;

    /** .Net class name. */
    private String clsName;

    /** Properties. */
    private Map<String, ?> props;

    /** Interop processor. */
    protected PlatformContext platformCtx;

    /** Pointer to native store. */
    protected long ptr;

    /**
     * Gets .NET assembly name.
     *
     * @return .NET assembly name.
     */
    public String getAssemblyName() {
        return assemblyName;
    }

    /**
     * Set .NET assembly name.
     *
     * @param assemblyName .NET assembly name.
     */
    public void setAssemblyName(String assemblyName) {
        this.assemblyName = assemblyName;
    }

    /**
     * Gets .NET class name.
     *
     * @return .NET class name.
     */
    public String getClassName() {
        return clsName;
    }

    /**
     * Sets .NET class name.
     *
     * @param clsName .NET class name.
     */
    public void setClassName(String clsName) {
        this.clsName = clsName;
    }

    /**
     * Get properties.
     *
     * @return Properties.
     */
    public Map<String, ?> getProperties() {
        return props;
    }

    /**
     * Set properties.
     *
     * @param props Properties.
     */
    public void setProperties(Map<String, ?> props) {
        this.props = props;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V load(final K key) {
        try {
            final GridTuple<V> val = new GridTuple<>();

            doInvoke(new IgniteInClosureX<PortableRawWriterEx>() {
                @Override public void applyx(PortableRawWriterEx writer) throws IgniteCheckedException {
                    writer.writeByte(OP_LOAD);
                    writer.writeLong(session());
                    writer.writeString(ses.cacheName());
                    writer.writeObject(key);
                }
            }, new LoadCallback<>(platformCtx, val));

            return val.get();
        }
        catch (IgniteCheckedException e) {
            throw new CacheLoaderException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> loadAll(final Iterable<? extends K> keys) {
        try {
            final Map<K, V> loaded = new HashMap<>();

            doInvoke(new IgniteInClosureX<PortableRawWriterEx>() {
                @Override public void applyx(PortableRawWriterEx writer) throws IgniteCheckedException {
                    writer.writeByte(OP_LOAD_ALL);
                    writer.writeLong(session());
                    writer.writeString(ses.cacheName());
                    writer.writeCollection((Collection) keys);
                }
            }, new LoadAllCallback<>(platformCtx, loaded));

            return loaded;
        }
        catch (IgniteCheckedException e) {
            throw new CacheLoaderException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(final IgniteBiInClosure<K, V> clo, final @Nullable Object... args) {
        try {
            doInvoke(new IgniteInClosureX<PortableRawWriterEx>() {
                @Override public void applyx(PortableRawWriterEx writer) throws IgniteCheckedException {
                    writer.writeByte(OP_LOAD_CACHE);
                    writer.writeLong(session());
                    writer.writeString(ses.cacheName());
                    writer.writeObjectArray(args);
                }
            }, new LoadCacheCallback<>(platformCtx, clo));
        }
        catch (IgniteCheckedException e) {
            throw new CacheLoaderException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(final Cache.Entry<? extends K, ? extends V> entry) {
        try {
            doInvoke(new IgniteInClosureX<PortableRawWriterEx>() {
                @Override public void applyx(PortableRawWriterEx writer) throws IgniteCheckedException {
                    writer.writeByte(OP_PUT);
                    writer.writeLong(session());
                    writer.writeString(ses.cacheName());
                    writer.writeObject(entry.getKey());
                    writer.writeObject(entry.getValue());
                }
            }, null);
        }
        catch (IgniteCheckedException e) {
            throw new CacheWriterException(U.convertExceptionNoWrap(e));
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"NullableProblems", "unchecked"})
    @Override public void writeAll(final Collection<Cache.Entry<? extends K, ? extends V>> entries) {
        try {
            doInvoke(new IgniteInClosureX<PortableRawWriterEx>() {
                @Override public void applyx(PortableRawWriterEx writer) throws IgniteCheckedException {
                    Map<K, V> map = new AbstractMap<K, V>() {
                        @Override public int size() {
                            return entries.size();
                        }

                        @Override public Set<Entry<K, V>> entrySet() {
                            return new AbstractSet<Entry<K, V>>() {
                                @Override public Iterator<Entry<K, V>> iterator() {
                                    return F.iterator(entries, new C1<Cache.Entry<? extends K, ? extends V>, Entry<K, V>>() {
                                        private static final long serialVersionUID = 0L;

                                        @Override public Entry<K, V> apply(Cache.Entry<? extends K, ? extends V> entry) {
                                            return new GridMapEntry<>(entry.getKey(), entry.getValue());
                                        }
                                    }, true);
                                }

                                @Override public int size() {
                                    return entries.size();
                                }
                            };
                        }
                    };

                    writer.writeByte(OP_PUT_ALL);
                    writer.writeLong(session());
                    writer.writeString(ses.cacheName());
                    writer.writeMap(map);
                }
            }, null);
        }
        catch (IgniteCheckedException e) {
            throw new CacheWriterException(U.convertExceptionNoWrap(e));
        }
    }

    /** {@inheritDoc} */
    @Override public void delete(final Object key) {
        try {
            doInvoke(new IgniteInClosureX<PortableRawWriterEx>() {
                @Override public void applyx(PortableRawWriterEx writer) throws IgniteCheckedException {
                    writer.writeByte(OP_RMV);
                    writer.writeLong(session());
                    writer.writeString(ses.cacheName());
                    writer.writeObject(key);
                }
            }, null);
        }
        catch (IgniteCheckedException e) {
            throw new CacheWriterException(U.convertExceptionNoWrap(e));
        }
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(final Collection<?> keys) {
        try {
            doInvoke(new IgniteInClosureX<PortableRawWriterEx>() {
                @Override public void applyx(PortableRawWriterEx writer) throws IgniteCheckedException {
                    writer.writeByte(OP_RMV_ALL);
                    writer.writeLong(session());
                    writer.writeString(ses.cacheName());
                    writer.writeCollection(keys);
                }
            }, null);
        }
        catch (IgniteCheckedException e) {
            throw new CacheWriterException(U.convertExceptionNoWrap(e));
        }
    }

    /** {@inheritDoc} */
    @Override public void sessionEnd(final boolean commit) {
        try {
            doInvoke(new IgniteInClosureX<PortableRawWriterEx>() {
                @Override public void applyx(PortableRawWriterEx writer) throws IgniteCheckedException {
                    writer.writeByte(OP_SES_END);
                    writer.writeLong(session());
                    writer.writeString(ses.cacheName());
                    writer.writeBoolean(commit);
                }
            }, null);
        }
        catch (IgniteCheckedException e) {
            throw new CacheWriterException(U.convertExceptionNoWrap(e));
        }
    }

    /**
     * Initialize the store.
     *
     * @param ctx Context.
     * @param convertPortable Convert portable flag.
     * @throws org.apache.ignite.IgniteCheckedException
     */
    public void initialize(GridKernalContext ctx, boolean convertPortable) throws IgniteCheckedException {
        A.notNull(assemblyName, "assemblyName");
        A.notNull(clsName, "clsName");

        platformCtx = PlatformUtils.platformContext(ctx.grid());

        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = platformCtx.writer(out);

            writer.writeString(assemblyName);
            writer.writeString(clsName);
            writer.writeBoolean(convertPortable);
            writer.writeMap(props);

            out.synchronize();

            ptr = platformCtx.gateway().cacheStoreCreate(mem.pointer());
        }
    }

    /**
     * Gets session pointer created in native platform.
     *
     * @return Session pointer.
     * @throws org.apache.ignite.IgniteCheckedException If failed.
     */
    private long session() throws IgniteCheckedException {
        Long sesPtr = (Long)ses.properties().get(KEY_SES);

        if (sesPtr == null) {
            // Session is not deployed yet, do that.
            sesPtr = platformCtx.gateway().cacheStoreSessionCreate(ptr);

            ses.properties().put(KEY_SES, sesPtr);
        }

        return sesPtr;
    }

    /**
     * Perform actual invoke.
     *
     * @param task Task.
     * @param cb Optional callback.
     * @return Result.
     * @throws org.apache.ignite.IgniteCheckedException If failed.
     */
    protected int doInvoke(IgniteInClosureX<PortableRawWriterEx> task, @Nullable PlatformCacheStoreCallback cb)
        throws IgniteCheckedException{
        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = platformCtx.writer(out);

            task.apply(writer);

            out.synchronize();

            return platformCtx.gateway().cacheStoreInvoke(ptr, mem.pointer(), cb);
        }
    }

    /**
     * Destroys interop-aware component.
     *
     * @param ctx Context.
     */
    public void destroy(GridKernalContext ctx) {
        assert ctx != null;

        platformCtx.gateway().cacheStoreDestroy(ptr);
    }

    /**
     * Load callback.
     */
    private static class LoadCallback<V> extends PlatformCacheStoreCallback {
        /** Value. */
        private final GridTuple<V> val;

        /**
         * Constructor.
         *
         * @param ctx Context.
         * @param val Value.
         */
        public LoadCallback(PlatformContext ctx, GridTuple<V> val) {
            super(ctx);

            this.val = val;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override protected void invoke0(PortableRawReaderEx reader) {
            val.set((V)reader.readObjectDetached());
        }
    }

    /**
     * Load callback.
     */
    private static class LoadAllCallback<K, V> extends PlatformCacheStoreCallback {
        /** Value. */
        private final Map<K, V> loaded;

        /**
         * Constructor.
         *
         * @param ctx Context.
         * @param loaded Map with loaded values.
         */
        public LoadAllCallback(PlatformContext ctx, Map<K, V> loaded) {
            super(ctx);

            this.loaded = loaded;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override protected void invoke0(PortableRawReaderEx reader) {
            loaded.put((K) reader.readObjectDetached(), (V) reader.readObjectDetached());
        }
    }

    /**
     * Load callback.
     */
    private static class LoadCacheCallback<K, V> extends PlatformCacheStoreCallback {
        /** Value. */
        private final IgniteBiInClosure<K, V> clo;

        /**
         * Constructor.
         *
         * @param ctx Context.
         * @param clo Closure.
         */
        public LoadCacheCallback(PlatformContext ctx, IgniteBiInClosure<K, V> clo) {
            super(ctx);

            this.clo = clo;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override protected void invoke0(PortableRawReaderEx reader) {
            clo.apply((K) reader.readObjectDetached(), (V) reader.readObjectDetached());
        }
    }
}