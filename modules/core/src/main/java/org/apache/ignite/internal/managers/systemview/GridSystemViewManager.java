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

package org.apache.ignite.internal.managers.systemview;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.systemview.walker.CacheGroupViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.CacheViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.ClientConnectionViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.ClusterNodeViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.ComputeTaskViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.ContinuousQueryViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.ScanQueryViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.ServiceViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.TransactionViewWalker;
import org.apache.ignite.spi.systemview.ReadOnlySystemViewRegistry;
import org.apache.ignite.spi.systemview.SystemViewExporterSpi;
import org.apache.ignite.spi.systemview.view.CacheGroupView;
import org.apache.ignite.spi.systemview.view.CacheView;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;
import org.apache.ignite.spi.systemview.view.ClusterNodeView;
import org.apache.ignite.spi.systemview.view.ComputeTaskView;
import org.apache.ignite.spi.systemview.view.ContinuousQueryView;
import org.apache.ignite.spi.systemview.view.ScanQueryView;
import org.apache.ignite.spi.systemview.view.ServiceView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.apache.ignite.spi.systemview.view.TransactionView;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.notifyListeners;

/**
 * This manager should provide {@link ReadOnlySystemViewRegistry} for each configured {@link SystemViewExporterSpi}.
 *
 * @see SystemView
 * @see SystemViewAdapter
 */
public class GridSystemViewManager extends GridManagerAdapter<SystemViewExporterSpi>
    implements ReadOnlySystemViewRegistry {
    /** Registered system views. */
    private final ConcurrentHashMap<String, SystemView<?>> systemViews = new ConcurrentHashMap<>();

    /** System views creation listeners. */
    private final List<Consumer<SystemView<?>>> viewCreationLsnrs = new CopyOnWriteArrayList<>();

    /** Registered walkers for view row. */
    private final Map<Class<?>, SystemViewRowAttributeWalker<?>> walkers = new HashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public GridSystemViewManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getSystemViewExporterSpi());

        registerWalker(CacheGroupView.class, new CacheGroupViewWalker());
        registerWalker(CacheView.class, new CacheViewWalker());
        registerWalker(ServiceView.class, new ServiceViewWalker());
        registerWalker(ComputeTaskView.class, new ComputeTaskViewWalker());
        registerWalker(ClientConnectionView.class, new ClientConnectionViewWalker());
        registerWalker(TransactionView.class, new TransactionViewWalker());
        registerWalker(ContinuousQueryView.class, new ContinuousQueryViewWalker());
        registerWalker(ClusterNodeView.class, new ClusterNodeViewWalker());
        registerWalker(ScanQueryView.class, new ScanQueryViewWalker());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        for (SystemViewExporterSpi spi : getSpis())
            spi.setSystemViewRegistry(this);

        startSpi();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();
    }

    /**
     * Registers {@link SystemView} instance.
     *
     * @param sysView System view.
     * @param <R> Row type.
     */
    public <R> void registerView(SystemView<R> sysView) {
        registerView0(sysView.name(), sysView);
    }

    /**
     * Registers {@link SystemViewAdapter} view which exports {@link Collection} content.
     *
     * @param name Name.
     * @param desc Description.
     * @param rowCls Row class.
     * @param data Data.
     * @param rowFunc value to row function.
     * @param <R> View row type.
     * @param <D> Collection data type.
     */
    public <R, D> void registerView(String name, String desc, Class<R> rowCls, Collection<D> data,
        Function<D, R> rowFunc) {
        registerView0(name, new SystemViewAdapter<>(name,
            desc,
            rowCls,
            (SystemViewRowAttributeWalker<R>)walkers.get(rowCls),
            data,
            rowFunc));
    }

    /**
     * Registers {@link SystemViewInnerCollectionsAdapter} view which exports container content.
     *
     * @param name Name.
     * @param desc Description.
     * @param rowCls Row class.
     * @param container Container of the data.
     * @param dataExtractor Data extractor function.
     * @param rowFunc Row function
     * @param <C> Container entry type.
     * @param <R> View row type.
     * @param <D> Collection data type.
     */
    public <C, R, D> void registerInnerCollectionView(String name, String desc, Class<R> rowCls,
        Collection<C> container, Function<C, Collection<D>> dataExtractor, BiFunction<C, D, R> rowFunc) {
        registerView0(name, new SystemViewInnerCollectionsAdapter<>(name,
            desc,
            rowCls,
            (SystemViewRowAttributeWalker<R>)walkers.get(rowCls),
            container,
            dataExtractor,
            rowFunc));
    }

    /**
     * Registers {@link SystemViewInnerCollectionsAdapter} view which exports container content.
     *
     * @param name Name.
     * @param desc Description.
     * @param rowCls Row class.
     * @param container Container of the data.
     * @param dataExtractor Data extractor function.
     * @param rowFunc Row function
     * @param <C> Container entry type.
     * @param <R> View row type.
     * @param <D> Collection data type.
     */
    public <C, R, D> void registerInnerArrayView(String name, String desc, Class<R> rowCls, Collection<C> container,
        Function<C, D[]> dataExtractor, BiFunction<C, D, R> rowFunc) {
        registerView0(name, new SystemViewInnerCollectionsAdapter<>(name,
            desc,
            rowCls,
            (SystemViewRowAttributeWalker<R>)walkers.get(rowCls),
            container,
            c -> Arrays.asList(dataExtractor.apply(c)),
            rowFunc));
    }

    /**
     * Registers view which exports {@link Collection} content provided by specified {@code Supplier}.
     *
     * @param name Name.
     * @param desc Description.
     * @param rowCls Row class.
     * @param dataSupplier Data supplier.
     * @param rowFunc value to row function.
     * @param <R> View row type.
     * @param <D> Collection data type.
     */
    public <R, D> void registerView(String name, String desc, Class<R> rowCls, Supplier<Collection<D>> dataSupplier,
        Function<D, R> rowFunc) {
        registerView0(name, new SystemViewAdapter<>(name,
            desc,
            rowCls,
            (SystemViewRowAttributeWalker<R>)walkers.get(rowCls),
            dataSupplier,
            rowFunc));
    }

    /**
     * Registers view.
     *
     * @param name Name.
     * @param sysView System view.
     */
    private void registerView0(String name, SystemView sysView) {
        SystemView<?> old = systemViews.putIfAbsent(name, sysView);

        assert old == null;

        notifyListeners(sysView, viewCreationLsnrs, log);
    }

    /**
     * @param name Name of the view.
     * @return List.
     */
    @Nullable public <R> SystemView<R> view(String name) {
        return (SystemView<R>)systemViews.get(name);
    }

    /**
     * Registers walker for specified class.
     *
     * @param rowClass Row class.
     * @param walker Walker.
     * @param <R> Row type.
     */
    public <R> void registerWalker(Class<R> rowClass, SystemViewRowAttributeWalker<R> walker) {
        walkers.put(rowClass, walker);
    }

    /** {@inheritDoc} */
    @Override public void addSystemViewCreationListener(Consumer<SystemView<?>> lsnr) {
        viewCreationLsnrs.add(lsnr);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<SystemView<?>> iterator() {
        return systemViews.values().iterator();
    }
}
