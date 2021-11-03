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

package org.apache.ignite.internal.configuration.util;

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.innerNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.leafNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.namedListNodeVisitor;
import static org.apache.ignite.internal.util.CollectionUtils.viewReadOnly;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicProperty;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ConfigurationNotificationsUtil {
    /**
     * Recursively notifies all public configuration listeners, accumulating resulting futures in {@code futures} list.
     *
     * @param oldInnerNode    Old configuration values root.
     * @param newInnerNode    New configuration values root.
     * @param cfgNode         Public configuration tree node corresponding to the current inner nodes.
     * @param storageRevision Storage revision.
     * @param futures         Write-only list of futures to accumulate results.
     */
    public static void notifyListeners(
            @Nullable InnerNode oldInnerNode,
            InnerNode newInnerNode,
            DynamicConfiguration<InnerNode, ?> cfgNode,
            long storageRevision,
            List<CompletableFuture<?>> futures
    ) {
        notifyListeners(oldInnerNode, newInnerNode, cfgNode, storageRevision, futures, List.of(), new HashMap<>());
    }
    
    /**
     * Recursively notifies all public configuration listeners, accumulating resulting futures in {@code futures} list.
     *
     * @param oldInnerNode    Old configuration values root.
     * @param newInnerNode    New configuration values root.
     * @param cfgNode         Public configuration tree node corresponding to the current inner nodes.
     * @param storageRevision Storage revision.
     * @param futures         Write-only list of futures to accumulate results.
     * @param anyConfigs      Current {@link NamedListConfiguration#any "any"} configurations.
     * @param eventConfigs    Configuration containers for {@link ConfigurationNotificationEvent}.
     */
    private static void notifyListeners(
            @Nullable InnerNode oldInnerNode,
            InnerNode newInnerNode,
            DynamicConfiguration<InnerNode, ?> cfgNode,
            long storageRevision,
            List<CompletableFuture<?>> futures,
            Collection<DynamicConfiguration<InnerNode, ?>> anyConfigs,
            Map<Class<? extends ConfigurationProperty>, ConfigurationContainer> eventConfigs
    ) {
        assert !(cfgNode instanceof NamedListConfiguration);
        
        if (oldInnerNode == null || oldInnerNode == newInnerNode) {
            return;
        }
        
        eventConfigs.computeIfAbsent(cfgNode.configType(), cls -> new ConfigurationContainer(null, cfgNode));
        
        for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
            notifyPublicListeners(
                    anyConfig.listeners(),
                    oldInnerNode,
                    newInnerNode,
                    storageRevision,
                    futures,
                    ConfigurationListener::onUpdate,
                    eventConfigs
            );
        }
        
        notifyPublicListeners(
                cfgNode.listeners(),
                oldInnerNode,
                newInnerNode,
                storageRevision,
                futures,
                ConfigurationListener::onUpdate,
                eventConfigs
        );
        
        // Polymorphic configuration type has changed.
        // At the moment, we do not separate common fields from fields of a specific polymorphic configuration,
        // so this may cause errors in the logic below, perhaps we will fix it later.
        if (oldInnerNode.schemaType() != newInnerNode.schemaType()) {
            return;
        }
        
        oldInnerNode.traverseChildren(new ConfigurationVisitor<Void>() {
            /** {@inheritDoc} */
            @Override
            public Void visitLeafNode(String key, Serializable oldLeaf) {
                Serializable newLeaf = newInnerNode.traverseChild(key, leafNodeVisitor(), true);
                
                if (newLeaf != oldLeaf) {
                    for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                        notifyPublicListeners(
                                dynamicProperty(anyConfig, key).listeners(),
                                oldLeaf,
                                newLeaf,
                                storageRevision,
                                futures,
                                ConfigurationListener::onUpdate,
                                eventConfigs
                        );
                    }
                    
                    notifyPublicListeners(
                            dynamicProperty(cfgNode, key).listeners(),
                            oldLeaf,
                            newLeaf,
                            storageRevision,
                            futures,
                            ConfigurationListener::onUpdate,
                            eventConfigs
                    );
                }
                
                return null;
            }
            
            /** {@inheritDoc} */
            @Override
            public Void visitInnerNode(String key, InnerNode oldNode) {
                InnerNode newNode = newInnerNode.traverseChild(key, innerNodeVisitor(), true);
                
                notifyListeners(
                        oldNode,
                        newNode,
                        dynamicConfig(cfgNode, key),
                        storageRevision,
                        futures,
                        viewReadOnly(anyConfigs, cfg -> dynamicConfig(cfg, key)),
                        eventConfigs
                );
                
                return null;
            }
            
            /** {@inheritDoc} */
            @Override
            public Void visitNamedListNode(String key, NamedListNode<?> oldNamedList) {
                var newNamedList = (NamedListNode<InnerNode>) newInnerNode.traverseChild(key, namedListNodeVisitor(), true);
                
                if (newNamedList != oldNamedList) {
                    for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                        notifyPublicListeners(
                                namedDynamicConfig(anyConfig, key).listeners(),
                                (NamedListView<InnerNode>) oldNamedList,
                                newNamedList,
                                storageRevision,
                                futures,
                                ConfigurationListener::onUpdate,
                                eventConfigs
                        );
                    }
                    
                    notifyPublicListeners(
                            namedDynamicConfig(cfgNode, key).listeners(),
                            (NamedListView<InnerNode>) oldNamedList,
                            newNamedList,
                            storageRevision,
                            futures,
                            ConfigurationListener::onUpdate,
                            eventConfigs
                    );
                    
                    // This is optimization, we could use "NamedListConfiguration#get" directly, but we don't want to.
                    List<String> oldNames = oldNamedList.namedListKeys();
                    List<String> newNames = newNamedList.namedListKeys();
                    
                    NamedListConfiguration<?, InnerNode, ?> namedListCfg = namedDynamicConfig(cfgNode, key);
                    
                    Map<String, ConfigurationProperty<?>> namedListCfgMembers = namedListCfg.touchMembers();
                    
                    Set<String> created = new HashSet<>(newNames);
                    created.removeAll(oldNames);
                    
                    Set<String> deleted = new HashSet<>(oldNames);
                    deleted.removeAll(newNames);
                    
                    Map<String, String> renamed = new HashMap<>();
                    if (!created.isEmpty() && !deleted.isEmpty()) {
                        Map<String, String> createdIds = new HashMap<>();
                        
                        for (String createdKey : created) {
                            createdIds.put(newNamedList.internalId(createdKey), createdKey);
                        }
                        
                        // Avoiding ConcurrentModificationException.
                        for (String deletedKey : Set.copyOf(deleted)) {
                            String internalId = oldNamedList.internalId(deletedKey);
                            
                            String maybeRenamedKey = createdIds.get(internalId);
                            
                            if (maybeRenamedKey == null) {
                                continue;
                            }
                            
                            deleted.remove(deletedKey);
                            created.remove(maybeRenamedKey);
                            renamed.put(deletedKey, maybeRenamedKey);
                        }
                    }
                    
                    // Lazy initialization.
                    Collection<DynamicConfiguration<InnerNode, ?>> newAnyConfigs = null;
                    
                    for (String name : created) {
                        DynamicConfiguration<InnerNode, ?> newNodeCfg =
                                (DynamicConfiguration<InnerNode, ?>) namedListCfg.members().get(name);
                        
                        touch(newNodeCfg);
                        
                        eventConfigs.put(newNodeCfg.configType(), new ConfigurationContainer(name, newNodeCfg));
                        
                        InnerNode newVal = newNamedList.getInnerNode(name);
                        
                        for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                            notifyPublicListeners(
                                    namedDynamicConfig(anyConfig, key).extendedListeners(),
                                    null,
                                    newVal,
                                    storageRevision,
                                    futures,
                                    ConfigurationNamedListListener::onCreate,
                                    eventConfigs
                            );
                        }
                        
                        notifyPublicListeners(
                                namedDynamicConfig(cfgNode, key).extendedListeners(),
                                null,
                                newVal,
                                storageRevision,
                                futures,
                                ConfigurationNamedListListener::onCreate,
                                eventConfigs
                        );
                        
                        if (newAnyConfigs == null) {
                            newAnyConfigs = mergeAnyConfigs(
                                    viewReadOnly(anyConfigs, cfg -> any(namedDynamicConfig(cfg, key))),
                                    any(namedListCfg)
                            );
                        }
                        
                        notifyAnyListenersOnCreate(
                                newVal,
                                newNodeCfg,
                                storageRevision,
                                futures,
                                newAnyConfigs,
                                eventConfigs
                        );
                        
                        eventConfigs.remove(newNodeCfg.configType());
                    }
                    
                    for (String name : deleted) {
                        DynamicConfiguration<InnerNode, ?> delNodeCfg =
                                (DynamicConfiguration<InnerNode, ?>) namedListCfgMembers.get(name);
                        
                        eventConfigs.put(delNodeCfg.configType(), new ConfigurationContainer(name, null));
                        
                        InnerNode oldVal = oldNamedList.getInnerNode(name);
                        
                        for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                            notifyPublicListeners(
                                    namedDynamicConfig(anyConfig, key).extendedListeners(),
                                    oldVal,
                                    null,
                                    storageRevision,
                                    futures,
                                    ConfigurationNamedListListener::onDelete,
                                    eventConfigs
                            );
                        }
                        
                        notifyPublicListeners(
                                namedDynamicConfig(cfgNode, key).extendedListeners(),
                                oldVal,
                                null,
                                storageRevision,
                                futures,
                                ConfigurationNamedListListener::onDelete,
                                eventConfigs
                        );
                        
                        // Notification for deleted configuration.
                        
                        for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                            notifyPublicListeners(
                                    any(namedDynamicConfig(anyConfig, key)).listeners(),
                                    oldVal,
                                    null,
                                    storageRevision,
                                    futures,
                                    ConfigurationListener::onUpdate,
                                    eventConfigs
                            );
                        }
                        
                        notifyPublicListeners(
                                delNodeCfg.listeners(),
                                oldVal,
                                null,
                                storageRevision,
                                futures,
                                ConfigurationListener::onUpdate,
                                eventConfigs
                        );
                        
                        eventConfigs.remove(delNodeCfg.configType());
                    }
                    
                    for (Map.Entry<String, String> entry : renamed.entrySet()) {
                        DynamicConfiguration<InnerNode, ?> renNodeCfg =
                                (DynamicConfiguration<InnerNode, ?>) namedListCfg.members().get(entry.getValue());
                        
                        eventConfigs.put(
                                renNodeCfg.configType(),
                                new ConfigurationContainer(entry.getValue(), renNodeCfg)
                        );
                        
                        InnerNode oldVal = oldNamedList.getInnerNode(entry.getKey());
                        InnerNode newVal = newNamedList.getInnerNode(entry.getValue());
                        
                        for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                            notifyPublicListeners(
                                    namedDynamicConfig(anyConfig, key).extendedListeners(),
                                    oldVal,
                                    newVal,
                                    storageRevision,
                                    futures,
                                    (listener, evt) -> listener.onRename(entry.getKey(), entry.getValue(), evt),
                                    eventConfigs
                            );
                        }
                        
                        notifyPublicListeners(
                                namedDynamicConfig(cfgNode, key).extendedListeners(),
                                oldVal,
                                newVal,
                                storageRevision,
                                futures,
                                (listener, evt) -> listener.onRename(entry.getKey(), entry.getValue(), evt),
                                eventConfigs
                        );
                        
                        eventConfigs.remove(renNodeCfg.configType());
                    }
                    
                    Set<String> updated = new HashSet<>(newNames);
                    updated.retainAll(oldNames);
                    
                    for (String name : updated) {
                        InnerNode oldVal = oldNamedList.getInnerNode(name);
                        InnerNode newVal = newNamedList.getInnerNode(name);
                        
                        if (oldVal == newVal) {
                            continue;
                        }
                        
                        DynamicConfiguration<InnerNode, ?> updNodeCfg =
                                (DynamicConfiguration<InnerNode, ?>) namedListCfgMembers.get(name);
                        
                        eventConfigs.put(updNodeCfg.configType(), new ConfigurationContainer(name, updNodeCfg));
                        
                        for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                            notifyPublicListeners(
                                    namedDynamicConfig(anyConfig, key).extendedListeners(),
                                    oldVal,
                                    newVal,
                                    storageRevision,
                                    futures,
                                    ConfigurationListener::onUpdate,
                                    eventConfigs
                            );
                        }
                        
                        notifyPublicListeners(
                                namedDynamicConfig(cfgNode, key).extendedListeners(),
                                oldVal,
                                newVal,
                                storageRevision,
                                futures,
                                ConfigurationListener::onUpdate,
                                eventConfigs
                        );
                        
                        if (newAnyConfigs == null) {
                            newAnyConfigs = mergeAnyConfigs(
                                    viewReadOnly(anyConfigs, cfg -> any(namedDynamicConfig(cfg, key))),
                                    any(namedListCfg)
                            );
                        }
                        
                        notifyListeners(
                                oldVal,
                                newVal,
                                updNodeCfg,
                                storageRevision,
                                futures,
                                newAnyConfigs,
                                eventConfigs
                        );
                    }
                }
                
                return null;
            }
        }, true);
        
        eventConfigs.remove(cfgNode.configType());
    }
    
    /**
     * Invoke {@link ConfigurationListener#onUpdate(ConfigurationNotificationEvent)} on all passed listeners and put results in {@code
     * futures}. Not recursively.
     *
     * @param listeners       Collection of listeners.
     * @param oldVal          Old configuration value.
     * @param newVal          New configuration value.
     * @param storageRevision Storage revision.
     * @param futures         Write-only list of futures.
     * @param updater         Update closure to be invoked on the listener instance.
     * @param <V>             Type of the node.
     * @param <L>             Type of the configuration listener.
     */
    private static <V, L extends ConfigurationListener<V>> void notifyPublicListeners(
            Collection<? extends L> listeners,
            V oldVal,
            V newVal,
            long storageRevision,
            List<CompletableFuture<?>> futures,
            BiFunction<L, ConfigurationNotificationEvent<V>, CompletableFuture<?>> updater,
            Map<Class<? extends ConfigurationProperty>, ConfigurationContainer> configs
    ) {
        if (listeners.isEmpty()) {
            return;
        }
        
        ConfigurationNotificationEvent<V> evt = new ConfigurationNotificationEventImpl<>(
                oldVal,
                newVal,
                storageRevision,
                configs
        );
        
        for (L listener : listeners) {
            try {
                CompletableFuture<?> future = updater.apply(listener, evt);
                
                assert future != null : updater;
                
                if (future.isCompletedExceptionally() || future.isCancelled() || !future.isDone()) {
                    futures.add(future);
                }
            } catch (Throwable t) {
                futures.add(CompletableFuture.failedFuture(t));
            }
        }
    }
    
    /**
     * Ensures that dynamic configuration tree is up to date and further notifications on it will be invoked correctly.
     *
     * @param cfg Dynamic configuration node instance.
     */
    public static void touch(DynamicConfiguration<?, ?> cfg) {
        cfg.touchMembers();
        
        for (ConfigurationProperty<?> value : cfg.members().values()) {
            if (value instanceof DynamicConfiguration) {
                touch((DynamicConfiguration<?, ?>) value);
            }
        }
    }
    
    /**
     * Recursive notification of all public listeners from the {@code anyConfigs} for the created naming list item, accumulating resulting
     * futures in {@code futures} list.
     *
     * @param innerNode       Configuration values root.
     * @param cfgNode         Public configuration tree node corresponding to the current inner nodes.
     * @param storageRevision Storage revision.
     * @param futures         Write-only list of futures to accumulate results.
     * @param anyConfigs      Current {@link NamedListConfiguration#any "any"} configurations.
     */
    private static void notifyAnyListenersOnCreate(
            InnerNode innerNode,
            DynamicConfiguration<InnerNode, ?> cfgNode,
            long storageRevision,
            List<CompletableFuture<?>> futures,
            Collection<DynamicConfiguration<InnerNode, ?>> anyConfigs,
            Map<Class<? extends ConfigurationProperty>, ConfigurationContainer> configs
    ) {
        assert !(cfgNode instanceof NamedListConfiguration);
        
        for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
            notifyPublicListeners(
                    anyConfig.listeners(),
                    null,
                    innerNode,
                    storageRevision,
                    futures,
                    ConfigurationListener::onUpdate,
                    configs
            );
        }
        
        innerNode.traverseChildren(new ConfigurationVisitor<Void>() {
            /** {@inheritDoc} */
            @Override
            public Void visitLeafNode(String key, Serializable newLeaf) {
                for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                    notifyPublicListeners(
                            dynamicProperty(anyConfig, key).listeners(),
                            null,
                            newLeaf,
                            storageRevision,
                            futures,
                            ConfigurationListener::onUpdate,
                            configs
                    );
                }
                
                return null;
            }
            
            /** {@inheritDoc} */
            @Override
            public Void visitInnerNode(String key, InnerNode newNode) {
                DynamicConfiguration<InnerNode, ?> innerNodeCfg = dynamicConfig(cfgNode, key);
                
                configs.put(innerNodeCfg.configType(), new ConfigurationContainer(null, innerNodeCfg));
                
                notifyAnyListenersOnCreate(
                        newNode,
                        innerNodeCfg,
                        storageRevision,
                        futures,
                        viewReadOnly(anyConfigs, cfg -> dynamicConfig(cfg, key)),
                        configs
                );
                
                configs.remove(innerNodeCfg.configType());
                
                return null;
            }
            
            /** {@inheritDoc} */
            @Override
            public Void visitNamedListNode(String key, NamedListNode<?> newNamedList) {
                for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                    notifyPublicListeners(
                            namedDynamicConfig(anyConfig, key).listeners(),
                            null,
                            (NamedListView<InnerNode>) newNamedList,
                            storageRevision,
                            futures,
                            ConfigurationListener::onUpdate,
                            configs
                    );
                }
                
                // Created only.
                
                // Lazy initialization.
                Collection<DynamicConfiguration<InnerNode, ?>> newAnyConfigs = null;
                
                for (String name : newNamedList.namedListKeys()) {
                    DynamicConfiguration<InnerNode, ?> newNodeCfg =
                            (DynamicConfiguration<InnerNode, ?>) namedDynamicConfig(cfgNode, key).get(name);
                    
                    configs.put(newNodeCfg.configType(), new ConfigurationContainer(name, newNodeCfg));
                    
                    InnerNode newVal = newNamedList.getInnerNode(name);
                    
                    for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                        notifyPublicListeners(
                                namedDynamicConfig(anyConfig, key).extendedListeners(),
                                null,
                                newVal,
                                storageRevision,
                                futures,
                                ConfigurationNamedListListener::onCreate,
                                configs
                        );
                    }
                    
                    if (newAnyConfigs == null) {
                        newAnyConfigs = mergeAnyConfigs(
                                viewReadOnly(anyConfigs, cfg -> any(namedDynamicConfig(cfg, key))),
                                any(namedDynamicConfig(cfgNode, key))
                        );
                    }
                    
                    notifyAnyListenersOnCreate(
                            newVal,
                            newNodeCfg,
                            storageRevision,
                            futures,
                            newAnyConfigs,
                            configs
                    );
                    
                    configs.remove(newNodeCfg.configType());
                }
                
                return null;
            }
        }, true);
    }
    
    /**
     * Merge {@link NamedListConfiguration#any "any"} configurations.
     *
     * @param anyConfigs Current {@link NamedListConfiguration#any "any"} configurations.
     * @param anyConfig  New {@link NamedListConfiguration#any "any"} configuration.
     * @return Merged {@link NamedListConfiguration#any "any"} configurations.
     */
    private static Collection<DynamicConfiguration<InnerNode, ?>> mergeAnyConfigs(
            Collection<DynamicConfiguration<InnerNode, ?>> anyConfigs,
            DynamicConfiguration<InnerNode, ?> anyConfig
    ) {
        if (anyConfigs.isEmpty()) {
            return List.of(anyConfig);
        } else {
            Collection<DynamicConfiguration<InnerNode, ?>> res = new ArrayList<>(anyConfigs.size() + 1);
            
            res.addAll(anyConfigs);
            res.add(anyConfig);
            
            return res;
        }
    }
    
    /**
     * Returns the dynamic property of the leaf.
     *
     * @param dynamicConfig Dynamic configuration.
     * @param nodeName      Name of the child node.
     * @return Dynamic property of a leaf.
     */
    private static DynamicProperty<Serializable> dynamicProperty(
            DynamicConfiguration<InnerNode, ?> dynamicConfig,
            String nodeName
    ) {
        return (DynamicProperty<Serializable>) dynamicConfig.members().get(nodeName);
    }
    
    /**
     * Returns the dynamic configuration of the child node.
     *
     * @param dynamicConfig Dynamic configuration.
     * @param nodeName      Name of the child node.
     * @return Dynamic configuration of the child node.
     */
    private static DynamicConfiguration<InnerNode, ?> dynamicConfig(
            DynamicConfiguration<InnerNode, ?> dynamicConfig,
            String nodeName
    ) {
        return (DynamicConfiguration<InnerNode, ?>) dynamicConfig.members().get(nodeName);
    }
    
    /**
     * Returns the named dynamic configuration of the child node.
     *
     * @param dynamicConfig Dynamic configuration.
     * @param nodeName      Name of the child node.
     * @return Named dynamic configuration of the child node.
     */
    private static NamedListConfiguration<?, InnerNode, ?> namedDynamicConfig(
            DynamicConfiguration<InnerNode, ?> dynamicConfig,
            String nodeName
    ) {
        return (NamedListConfiguration<?, InnerNode, ?>) dynamicConfig.members().get(nodeName);
    }
    
    /**
     * Returns the dynamic configuration of the {@link NamedListConfiguration#any any} node.
     *
     * @param namedConfig Dynamic configuration.
     * @return Dynamic configuration of the "any" node.
     */
    private static DynamicConfiguration<InnerNode, ?> any(NamedListConfiguration<?, InnerNode, ?> namedConfig) {
        return (DynamicConfiguration<InnerNode, ?>) namedConfig.any();
    }
}
