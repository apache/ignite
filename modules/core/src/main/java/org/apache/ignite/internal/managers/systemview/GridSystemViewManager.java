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
import org.apache.ignite.internal.managers.systemview.walker.StripedExecutorTaskViewWalker;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.StripedExecutor.Stripe;
import org.apache.ignite.spi.systemview.ReadOnlySystemViewRegistry;
import org.apache.ignite.spi.systemview.SystemViewExporterSpi;
import org.apache.ignite.spi.systemview.view.StripedExecutorTaskView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.IgniteUtils.notifyListeners;

/**
 * This manager should provide {@link ReadOnlySystemViewRegistry} for each configured {@link SystemViewExporterSpi}.
 *
 * @see SystemView
 * @see SystemViewAdapter
 */
public class GridSystemViewManager extends GridManagerAdapter<SystemViewExporterSpi>
    implements ReadOnlySystemViewRegistry {
    /** Name of the system view for a system {@link StripedExecutor} queue view. */
    public static final String SYS_POOL_QUEUE_VIEW = metricName("striped", "threadpool", "queue");

    /** Description of the system view for a system {@link StripedExecutor} queue view. */
    public static final String SYS_POOL_QUEUE_VIEW_DESC = "Striped thread pool task queue";

    /** Name of the system view for a data streamer {@link StripedExecutor} queue view. */
    public static final String STREAM_POOL_QUEUE_VIEW = metricName("datastream", "threadpool", "queue");

    /** Description of the system view for a data streamer {@link StripedExecutor} queue view. */
    public static final String STREAM_POOL_QUEUE_VIEW_DESC = "Datastream thread pool task queue";

    /** Registered system views. */
    private final ConcurrentHashMap<String, SystemView<?>> systemViews = new ConcurrentHashMap<>();

    /** System views creation listeners. */
    private final List<Consumer<SystemView<?>>> viewCreationLsnrs = new CopyOnWriteArrayList<>();

    /**
     * @param ctx Kernal context.
     */
    public GridSystemViewManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getSystemViewExporterSpi());
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
     * Registers system views for a striped thread pools.
     *
     * @param stripedExecSvc Striped executor.
     * @param dataStreamExecSvc Data streamer executor service.
     */
    public void registerThreadPools(StripedExecutor stripedExecSvc, StripedExecutor dataStreamExecSvc) {
        ctx.systemView().registerInnerCollectionView(SYS_POOL_QUEUE_VIEW, SYS_POOL_QUEUE_VIEW_DESC,
            new StripedExecutorTaskViewWalker(),
            Arrays.asList(stripedExecSvc.stripes()),
            Stripe::queue,
            StripedExecutorTaskView::new);

        ctx.systemView().registerInnerCollectionView(STREAM_POOL_QUEUE_VIEW, STREAM_POOL_QUEUE_VIEW_DESC,
            new StripedExecutorTaskViewWalker(),
            Arrays.asList(dataStreamExecSvc.stripes()),
            Stripe::queue,
            StripedExecutorTaskView::new);
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
     * @param walker Row walker.
     * @param data Data.
     * @param rowFunc value to row function.
     * @param <R> View row type.
     * @param <D> Collection data type.
     */
    public <R, D> void registerView(String name, String desc, SystemViewRowAttributeWalker<R> walker,
        Collection<D> data, Function<D, R> rowFunc) {
        registerView0(name, new SystemViewAdapter<>(name,
            desc,
            walker,
            data,
            rowFunc));
    }

    /**
     * Registers {@link SystemViewInnerCollectionsAdapter} view which exports container content.
     *
     * @param name Name.
     * @param desc Description.
     * @param walker Row walker.
     * @param container Container of the data.
     * @param dataExtractor Data extractor function.
     * @param rowFunc Row function
     * @param <C> Container entry type.
     * @param <R> View row type.
     * @param <D> Collection data type.
     */
    public <C, R, D> void registerInnerCollectionView(String name, String desc, SystemViewRowAttributeWalker<R> walker,
        Iterable<C> container, Function<C, Collection<D>> dataExtractor, BiFunction<C, D, R> rowFunc) {
        registerView0(name, new SystemViewInnerCollectionsAdapter<>(name,
            desc,
            walker,
            container,
            dataExtractor,
            rowFunc));
    }

    /**
     * Registers {@link SystemViewInnerCollectionsAdapter} view which exports container content.
     *
     * @param name Name.
     * @param desc Description.
     * @param walker Row walker.
     * @param container Container of the data.
     * @param dataExtractor Data extractor function.
     * @param rowFunc Row function
     * @param <C> Container entry type.
     * @param <R> View row type.
     * @param <D> Collection data type.
     */
    public <C, R, D> void registerInnerArrayView(String name, String desc, SystemViewRowAttributeWalker<R> walker,
        Collection<C> container, Function<C, D[]> dataExtractor, BiFunction<C, D, R> rowFunc) {
        registerView0(name, new SystemViewInnerCollectionsAdapter<>(name,
            desc,
            walker,
            container,
            c -> Arrays.asList(dataExtractor.apply(c)),
            rowFunc));
    }

    /**
     * Registers view which exports {@link Collection} content provided by specified {@code Supplier}.
     *
     * @param name Name.
     * @param desc Description.
     * @param walker Row walker.
     * @param dataSupplier Data supplier.
     * @param rowFunc value to row function.
     * @param <R> View row type.
     * @param <D> Collection data type.
     */
    public <R, D> void registerView(String name, String desc, SystemViewRowAttributeWalker<R> walker,
        Supplier<Collection<D>> dataSupplier, Function<D, R> rowFunc) {
        registerView0(name, new SystemViewAdapter<>(name,
            desc,
            walker,
            dataSupplier,
            rowFunc));
    }

    /**
     * Registers {@link FiltrableSystemViewAdapter} view with content filtering capabilities.
     *
     * @param name Name.
     * @param desc Description.
     * @param walker Row walker.
     * @param dataSupplier Data supplier with content filtering capabilities.
     * @param rowFunc Row function
     * @param <R> View row type.
     * @param <D> Collection data type.
     */
    public <R, D> void registerFiltrableView(String name, String desc, SystemViewRowAttributeWalker<R> walker,
        Function<Map<String, Object>, Iterable<D>> dataSupplier, Function<D, R> rowFunc) {
        registerView0(name, new FiltrableSystemViewAdapter<>(name,
            desc,
            walker,
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
        systemViews.put(name, sysView);

        notifyListeners(sysView, viewCreationLsnrs, log);
    }

    /**
     * @param name Name of the view.
     * @return List.
     */
    @Nullable public <R> SystemView<R> view(String name) {
        return (SystemView<R>)systemViews.get(name);
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
