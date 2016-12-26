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
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.cache.store.PlatformCacheStore;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
public class PlatformDotNetCacheStore<K, V> implements CacheStore<K, V>, PlatformCacheStore, LifecycleAware {
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

    /** .Net class name. */
    private String typName;

    /** Properties. */
    private Map<String, ?> props;

    /** Native factory. */
    @GridToStringInclude
    private final Object nativeFactory;

    /** Interop processor. */
    @GridToStringExclude
    protected PlatformContext platformCtx;

    /** Pointer to native store. */
    @GridToStringExclude
    protected long ptr;

    /**
     * Default ctor.
     */
    public PlatformDotNetCacheStore() {
        nativeFactory = null;
    }

    /**
     * Native factory ctor.
     */
    public PlatformDotNetCacheStore(Object nativeFactory) {
        assert nativeFactory != null;

        this.nativeFactory = nativeFactory;
    }

    /**
     * Gets .NET class name.
     *
     * @return .NET class name.
     */
    public String getTypeName() {
        return typName;
    }

    /**
     * Sets .NET class name.
     *
     * @param typName .NET class name.
     */
    public void setTypeName(String typName) {
        this.typName = typName;
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

            doInvoke(new IgniteInClosureX<BinaryRawWriterEx>() {
                @Override public void applyx(BinaryRawWriterEx writer) throws IgniteCheckedException {
                    writer.writeByte(OP_LOAD);
                    writer.writeLong(session());
                    writer.writeString(ses.cacheName());
                    writer.writeObject(key);
                }
            }, new IgniteInClosureX<BinaryRawReaderEx>() {
                @Override public void applyx(BinaryRawReaderEx reader) {
                    val.set((V)reader.readObjectDetached());
                }
            });

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

            final Collection keys0 = (Collection)keys;

            doInvoke(new IgniteInClosureX<BinaryRawWriterEx>() {
                @Override public void applyx(BinaryRawWriterEx writer) throws IgniteCheckedException {
                    writer.writeByte(OP_LOAD_ALL);
                    writer.writeLong(session());
                    writer.writeString(ses.cacheName());
                    writer.writeCollection(keys0);
                }
            }, new IgniteInClosureX<BinaryRawReaderEx>() {
                @Override public void applyx(BinaryRawReaderEx reader) {
                    int cnt = reader.readInt();

                    for (int i = 0; i < cnt; i++)
                        loaded.put((K) reader.readObjectDetached(), (V) reader.readObjectDetached());
                }
            });

            return loaded;
        }
        catch (IgniteCheckedException e) {
            throw new CacheLoaderException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(final IgniteBiInClosure<K, V> clo, final @Nullable Object... args) {
        try {
            doInvoke(new IgniteInClosureX<BinaryRawWriterEx>() {
                @Override public void applyx(BinaryRawWriterEx writer) throws IgniteCheckedException {
                    writer.writeByte(OP_LOAD_CACHE);
                    writer.writeLong(session());
                    writer.writeString(ses.cacheName());
                    writer.writeObjectArray(args);
                }
            }, new IgniteInClosureX<BinaryRawReaderEx>() {
                @Override public void applyx(BinaryRawReaderEx reader) {
                    int cnt = reader.readInt();

                    for (int i = 0; i < cnt; i++)
                        clo.apply((K) reader.readObjectDetached(), (V) reader.readObjectDetached());
                }
            });
        }
        catch (IgniteCheckedException e) {
            throw new CacheLoaderException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(final Cache.Entry<? extends K, ? extends V> entry) {
        try {
            doInvoke(new IgniteInClosureX<BinaryRawWriterEx>() {
                @Override public void applyx(BinaryRawWriterEx writer) throws IgniteCheckedException {
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
        assert entries != null;
        try {
            doInvoke(new IgniteInClosureX<BinaryRawWriterEx>() {
                @Override public void applyx(BinaryRawWriterEx writer) throws IgniteCheckedException {
                    writer.writeByte(OP_PUT_ALL);
                    writer.writeLong(session());
                    writer.writeString(ses.cacheName());

                    writer.writeInt(entries.size());

                    for (Cache.Entry<? extends K, ? extends V> e : entries) {
                        writer.writeObject(e.getKey());
                        writer.writeObject(e.getValue());
                    }
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
            doInvoke(new IgniteInClosureX<BinaryRawWriterEx>() {
                @Override public void applyx(BinaryRawWriterEx writer) throws IgniteCheckedException {
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
            doInvoke(new IgniteInClosureX<BinaryRawWriterEx>() {
                @Override public void applyx(BinaryRawWriterEx writer) throws IgniteCheckedException {
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
            doInvoke(new IgniteInClosureX<BinaryRawWriterEx>() {
                @Override public void applyx(BinaryRawWriterEx writer) throws IgniteCheckedException {
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

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        assert platformCtx != null;

        platformCtx.gateway().cacheStoreDestroy(ptr);
    }

    /**
     * Initialize the store.
     *
     * @param ctx Context.
     * @param convertBinary Convert binary flag.
     * @throws org.apache.ignite.IgniteCheckedException
     */
    public void initialize(GridKernalContext ctx, boolean convertBinary) throws IgniteCheckedException {
        A.ensure(typName != null || nativeFactory != null,
                "Either typName or nativeFactory must be set in PlatformDotNetCacheStore");

        platformCtx = PlatformUtils.platformContext(ctx.grid());

        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = platformCtx.writer(out);

            write(writer, convertBinary);

            out.synchronize();

            ptr = platformCtx.gateway().cacheStoreCreate(mem.pointer());
        }
    }

    /**
     * Write store data to a stream.
     *
     * @param writer Writer.
     * @param convertBinary Convert binary flag.
     */
    protected void write(BinaryRawWriterEx writer, boolean convertBinary) {
        writer.writeBoolean(convertBinary);
        writer.writeObjectDetached(nativeFactory);

        if (nativeFactory == null) {
            writer.writeString(typName);
            writer.writeMap(props);
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
            sesPtr = platformCtx.gateway().cacheStoreSessionCreate();

            ses.properties().put(KEY_SES, sesPtr);
        }

        return sesPtr;
    }

    /**
     * Perform actual invoke.
     *
     * @param task Task.
     * @param readClo Reader.
     * @return Result.
     * @throws org.apache.ignite.IgniteCheckedException If failed.
     */
    protected int doInvoke(IgniteInClosure<BinaryRawWriterEx> task, IgniteInClosure<BinaryRawReaderEx> readClo)
        throws IgniteCheckedException{
        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = platformCtx.writer(out);

            writer.writeLong(ptr);

            task.apply(writer);

            out.synchronize();

            int res = platformCtx.gateway().cacheStoreInvoke(mem.pointer());

            if (res != 0) {
                // Read error
                Object nativeErr = platformCtx.reader(mem.input()).readObjectDetached();

                throw platformCtx.createNativeException(nativeErr);
            }

            if (readClo != null) {
                BinaryRawReaderEx reader = platformCtx.reader(mem);

                readClo.apply(reader);
            }

            return res;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformDotNetCacheStore.class, this);
    }
}
