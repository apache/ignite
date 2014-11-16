/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.plugin;

import org.gridgain.grid.design.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;
import java.util.*;

/**
 *
 */
public class IgnitePluginManager {

    /** */
    private final GridKernalContext ctx;

    /** */
    private final Map<String, PluginProvider> plugins = new LinkedHashMap<>();

    /** */
    private final Map<PluginProvider, GridPluginContext> pluginCtxMap = new IdentityHashMap<>();

    /**
     *
     * @param ctx
     * @param cfgs
     */
    public IgnitePluginManager(GridKernalContext ctx, @Nullable Collection<? extends PluginConfiguration> cfgs) {
        this.ctx = ctx;

        if (cfgs == null)
            return;

        for (PluginConfiguration pluginCfg : cfgs) {
            try {
                if (pluginCfg.providerClass() == null)
                    throw new IgniteException("Provider class is null.");

                GridPluginContext pluginCtx = new GridPluginContext(ctx, pluginCfg);

                PluginProvider provider;

                try {
                    Constructor<? extends  PluginProvider> ctr =
                        pluginCfg.providerClass().getConstructor(pluginCfg.getClass());

                    provider = ctr.newInstance(pluginCfg);
                }
                catch (NoSuchMethodException ignore) {
                    provider = pluginCfg.providerClass().newInstance();
                }

                if (F.isEmpty(provider.name()))
                    throw new IgniteException("Plugin name can not be empty.");

                if (provider.plugin() == null)
                    throw new IgniteException("Plugin is null.");

                if (plugins.containsKey(provider.name()))
                    throw new IgniteException("Duplicated plugin name: " + provider.name());

                plugins.put(provider.name(), provider);

                pluginCtxMap.put(provider, pluginCtx);
            }
            catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new IgniteException("Failed to create plugin provider instance.", e);
            }
        }
    }

    /**
     * @param name Plugin name.
     * @return Plugin provider.
     */
    @Nullable public PluginProvider pluginProvider(String name) {
        return plugins.get(name);
    }

    /**
     * @return All plugin providers.
     */
    public Collection<PluginProvider> allProviders() {
        return plugins.values();
    }

    /**
     * @param provider Plugin context.
     * @return
     */
    public PluginContext pluginContextForProvider(PluginProvider provider) {
        return pluginCtxMap.get(provider);
    }

    /**
     * @param cls
     * @param <T>
     * @return
     */
    public <T> T createComponent(Class<T> cls) {
        for (PluginProvider plugin : plugins.values()) {
            T comp = (T)plugin.createComponent(cls);

            if (comp != null)
                return comp;
        }

        return null;
    }
}
