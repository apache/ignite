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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.systemview.walker.CacheGroupViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.CacheViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.ClientConnectionViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.ComputeTaskViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.ServiceViewWalker;
import org.apache.ignite.spi.systemview.ReadOnlySystemViewRegistry;
import org.apache.ignite.spi.systemview.SystemViewExporterSpi;
import org.apache.ignite.spi.systemview.view.CacheGroupView;
import org.apache.ignite.spi.systemview.view.CacheView;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;
import org.apache.ignite.spi.systemview.view.ComputeTaskView;
import org.apache.ignite.spi.systemview.view.ServiceView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.odbc.ClientListenerAbstractConnectionContext.CLI_CONN_SYS_VIEW;
import static org.apache.ignite.internal.processors.odbc.ClientListenerAbstractConnectionContext.CLI_CONN_SYS_VIEW_DESC;
import static org.apache.ignite.internal.util.IgniteUtils.notifyListeners;

/**
 * This manager should provide {@link ReadOnlySystemViewRegistry} for each configured {@link SystemViewExporterSpi}.
 *
 * @see SystemView
 * @see SystemViewAdapter
 * @see SystemViewMap
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
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        for (SystemViewExporterSpi spi : getSpis())
            spi.setSystemViewRegistry(this);

        startSpi();

        registerMapView(CLI_CONN_SYS_VIEW, CLI_CONN_SYS_VIEW_DESC, ClientConnectionView.class);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();
    }

    /**
     * Registers view which exports {@link Collection} content.
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
        doRegister(name, new SystemViewAdapter<>(name,
            desc,
            rowCls,
            (SystemViewRowAttributeWalker<R>)walkers.get(rowCls),
            data,
            rowFunc));
    }

    /**
     * Creates system view which exports underlying {@link ConcurrentMap#values()} content.
     *
     * @param name Name of the view.
     * @param desc Description of the view.
     * @param rowCls Row class.
     * @param <K> Type of the key.
     * @param <R> Type of the row.
     * @return System view.
     */
    public <K, R> void registerMapView(String name, String desc, Class<R> rowCls) {
        doRegister(name, new SystemViewMap<K, R>(name,
            desc,
            rowCls,
            (SystemViewRowAttributeWalker<R>)walkers.get(rowCls)));
    }

    /** Puts view to the registry and notifies listeners */
    private void doRegister(String name, SystemView sysView) {
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
