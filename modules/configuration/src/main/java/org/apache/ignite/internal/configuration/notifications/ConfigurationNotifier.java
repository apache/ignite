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

package org.apache.ignite.internal.configuration.notifications;

import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.any;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.dynamicConfig;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.dynamicProperty;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.extendedListeners;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.listeners;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.mergeAnyConfigs;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.namedDynamicConfig;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.innerNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.leafNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.namedListNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.touch;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.apache.ignite.internal.util.CollectionUtils.viewReadOnly;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.ConfigurationNode;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.jetbrains.annotations.Nullable;

/**
 * Class for notifying configuration listeners.
 */
public class ConfigurationNotifier {
    /**
     * Recursive notification of all configuration listeners.
     *
     * <p>NOTE: If {@code oldInnerNode == null}, then {@link ConfigurationListener#onUpdate} and
     * {@link ConfigurationNamedListListener#onCreate} will be called and the value will only be in
     * {@link ConfigurationNotificationEvent#newValue}.
     *
     * @param oldInnerNode Old configuration values root.
     * @param newInnerNode New configuration values root.
     * @param config Public configuration tree node corresponding to the current inner nodes.
     * @param storageRevision Storage revision.
     * @return Collected configuration listener futures.
     * @see ConfigurationListener
     * @see ConfigurationNamedListListener
     */
    public static Collection<CompletableFuture<?>> notifyListeners(
            @Nullable InnerNode oldInnerNode,
            InnerNode newInnerNode,
            DynamicConfiguration<InnerNode, ?> config,
            long storageRevision
    ) {
        if (oldInnerNode == newInnerNode) {
            return List.of();
        }

        ConfigurationNotificationContext notificationCtx = new ConfigurationNotificationContext(storageRevision);

        notificationCtx.addContainer(config, null);

        if (oldInnerNode == null) {
            notifyListeners(newInnerNode, config, List.of(), notificationCtx);
        } else {
            notifyListeners(oldInnerNode, newInnerNode, config, List.of(), notificationCtx);
        }

        notificationCtx.removeContainer(config);

        return notificationCtx.futures;
    }

    private static void notifyListeners(
            @Nullable InnerNode oldInnerNode,
            InnerNode newInnerNode,
            DynamicConfiguration<InnerNode, ?> config,
            Collection<DynamicConfiguration<InnerNode, ?>> anyConfigs,
            ConfigurationNotificationContext notificationCtx
    ) {
        assert !(config instanceof NamedListConfiguration);

        if (oldInnerNode == null || newInnerNode == oldInnerNode) {
            return;
        }

        notifyPublicListeners(
                config.listeners(),
                viewReadOnly(anyConfigs, ConfigurationNode::listeners),
                oldInnerNode.specificNode(),
                newInnerNode.specificNode(),
                notificationCtx,
                ConfigurationListener::onUpdate
        );

        // Polymorphic configuration type has changed.
        // At the moment, we do not separate common fields from fields of a specific polymorphic configuration,
        // so this may cause errors in the logic below, perhaps we will fix it later.
        // TODO: https://issues.apache.org/jira/browse/IGNITE-15916
        if (oldInnerNode.schemaType() != newInnerNode.schemaType()) {
            return;
        }

        oldInnerNode.traverseChildren(new ConfigurationVisitor<Void>() {
            /** {@inheritDoc} */
            @Override
            public Void visitLeafNode(String key, Serializable oldLeaf) {
                Serializable newLeaf = newInnerNode.traverseChild(key, leafNodeVisitor(), true);

                if (newLeaf != oldLeaf) {
                    notifyPublicListeners(
                            listeners(dynamicProperty(config, key)),
                            viewReadOnly(anyConfigs, anyConfig -> listeners(dynamicProperty(anyConfig, key))),
                            oldLeaf,
                            newLeaf,
                            notificationCtx,
                            ConfigurationListener::onUpdate
                    );
                }

                return null;
            }

            /** {@inheritDoc} */
            @Override
            public Void visitInnerNode(String key, InnerNode oldNode) {
                InnerNode newNode = newInnerNode.traverseChild(key, innerNodeVisitor(), true);

                DynamicConfiguration<InnerNode, ?> newConfig = dynamicConfig(config, key);

                notificationCtx.addContainer(newConfig, null);

                notifyListeners(
                        oldNode,
                        newNode,
                        newConfig,
                        viewReadOnly(anyConfigs, anyConfig -> dynamicConfig(anyConfig, key)),
                        notificationCtx
                );

                notificationCtx.removeContainer(newConfig);

                return null;
            }

            /** {@inheritDoc} */
            @Override
            public Void visitNamedListNode(String key, NamedListNode<?> oldNamedList) {
                NamedListNode<InnerNode> newNamedList =
                        (NamedListNode<InnerNode>) newInnerNode.traverseChild(key, namedListNodeVisitor(), true);

                if (newNamedList != oldNamedList) {
                    notifyPublicListeners(
                            listeners(namedDynamicConfig(config, key)),
                            viewReadOnly(anyConfigs, anyConfig -> listeners(namedDynamicConfig(anyConfig, key))),
                            oldNamedList,
                            newNamedList,
                            notificationCtx,
                            ConfigurationListener::onUpdate
                    );

                    NamedListChanges namedListChanges = NamedListChanges.of(oldNamedList, newNamedList);

                    NamedListConfiguration<?, InnerNode, ?> namedListCfg = namedDynamicConfig(config, key);

                    Map<String, ConfigurationProperty<?>> namedListCfgMembers = namedListCfg.touchMembers();

                    // Lazy initialization.
                    Collection<DynamicConfiguration<InnerNode, ?>> newAnyConfigs = null;

                    for (String name : namedListChanges.created) {
                        DynamicConfiguration<InnerNode, ?> newNodeCfg =
                                (DynamicConfiguration<InnerNode, ?>) namedListCfg.members().get(name);

                        touch(newNodeCfg);

                        notificationCtx.addContainer(newNodeCfg, name);

                        InnerNode newVal = newNamedList.getInnerNode(name);

                        notifyPublicListeners(
                                extendedListeners(namedDynamicConfig(config, key)),
                                viewReadOnly(anyConfigs, anyConfig -> extendedListeners(namedDynamicConfig(anyConfig, key))),
                                null,
                                newVal.specificNode(),
                                notificationCtx,
                                ConfigurationNamedListListener::onCreate
                        );

                        if (newAnyConfigs == null) {
                            newAnyConfigs = mergeAnyConfigs(
                                    viewReadOnly(anyConfigs, anyConfig -> any(namedDynamicConfig(anyConfig, key))),
                                    any(namedListCfg)
                            );
                        }

                        notifyListeners(
                                newVal,
                                newNodeCfg,
                                newAnyConfigs,
                                notificationCtx
                        );

                        notificationCtx.removeContainer(newNodeCfg);
                    }

                    for (String name : namedListChanges.deleted) {
                        DynamicConfiguration<InnerNode, ?> delNodeCfg =
                                (DynamicConfiguration<InnerNode, ?>) namedListCfgMembers.get(name);

                        delNodeCfg.removedFromNamedList();

                        notificationCtx.addContainer(delNodeCfg, name);

                        InnerNode oldVal = oldNamedList.getInnerNode(name);

                        notifyPublicListeners(
                                extendedListeners(namedDynamicConfig(config, key)),
                                viewReadOnly(anyConfigs, anyConfig -> extendedListeners(namedDynamicConfig(anyConfig, key))),
                                oldVal.specificNode(),
                                null,
                                notificationCtx,
                                ConfigurationNamedListListener::onDelete
                        );

                        // Notification for deleted configuration.

                        notifyPublicListeners(
                                listeners(delNodeCfg),
                                viewReadOnly(anyConfigs, anyConfig -> listeners(any(namedDynamicConfig(anyConfig, key)))),
                                oldVal.specificNode(),
                                null,
                                notificationCtx,
                                ConfigurationListener::onUpdate
                        );

                        notificationCtx.removeContainer(delNodeCfg);
                    }

                    for (Map.Entry<String, String> entry : namedListChanges.renamed.entrySet()) {
                        DynamicConfiguration<InnerNode, ?> renNodeCfg =
                                (DynamicConfiguration<InnerNode, ?>) namedListCfg.members().get(entry.getValue());

                        notificationCtx.addContainer(renNodeCfg, entry.getValue());

                        InnerNode oldVal = oldNamedList.getInnerNode(entry.getKey());
                        InnerNode newVal = newNamedList.getInnerNode(entry.getValue());

                        notifyPublicListeners(
                                extendedListeners(namedDynamicConfig(config, key)),
                                viewReadOnly(anyConfigs, anyConfig -> extendedListeners(namedDynamicConfig(anyConfig, key))),
                                oldVal.specificNode(),
                                newVal.specificNode(),
                                notificationCtx,
                                (listener, event) -> listener.onRename(entry.getKey(), entry.getValue(), event)
                        );

                        notificationCtx.removeContainer(renNodeCfg);
                    }

                    for (String name : namedListChanges.updated) {
                        InnerNode oldVal = oldNamedList.getInnerNode(name);
                        InnerNode newVal = newNamedList.getInnerNode(name);

                        if (oldVal == newVal) {
                            continue;
                        }

                        DynamicConfiguration<InnerNode, ?> updNodeCfg =
                                (DynamicConfiguration<InnerNode, ?>) namedListCfgMembers.get(name);

                        notificationCtx.addContainer(updNodeCfg, name);

                        notifyPublicListeners(
                                extendedListeners(namedDynamicConfig(config, key)),
                                viewReadOnly(anyConfigs, anyConfig -> extendedListeners(namedDynamicConfig(anyConfig, key))),
                                oldVal.specificNode(),
                                newVal.specificNode(),
                                notificationCtx,
                                ConfigurationNamedListListener::onUpdate
                        );

                        if (newAnyConfigs == null) {
                            newAnyConfigs = mergeAnyConfigs(
                                    viewReadOnly(anyConfigs, anyConfig -> any(namedDynamicConfig(anyConfig, key))),
                                    any(namedListCfg)
                            );
                        }

                        notifyListeners(
                                oldVal,
                                newVal,
                                updNodeCfg,
                                newAnyConfigs,
                                notificationCtx
                        );

                        notificationCtx.removeContainer(updNodeCfg);
                    }
                }

                return null;
            }
        }, true);
    }

    /**
     * Recursive notification of all configuration listeners.
     *
     * <p>NOTE: Only {@link ConfigurationListener#onUpdate} and {@link ConfigurationNamedListListener#onCreate} will be called.
     *
     * <p>NOTE: Value will only be in {@link ConfigurationNotificationEvent#newValue}.
     */
    private static void notifyListeners(
            InnerNode innerNode,
            DynamicConfiguration<InnerNode, ?> config,
            Collection<DynamicConfiguration<InnerNode, ?>> anyConfigs,
            ConfigurationNotificationContext notificationCtx
    ) {
        assert !(config instanceof NamedListConfiguration);

        notifyPublicListeners(
                config.listeners(),
                viewReadOnly(anyConfigs, ConfigurationNode::listeners),
                null,
                innerNode.specificNode(),
                notificationCtx,
                ConfigurationListener::onUpdate
        );

        innerNode.traverseChildren(new ConfigurationVisitor<Void>() {
            /** {@inheritDoc} */
            @Override
            public Void visitLeafNode(String key, Serializable leaf) {
                notifyPublicListeners(
                        listeners(dynamicProperty(config, key)),
                        viewReadOnly(anyConfigs, anyConfig -> listeners(dynamicProperty(anyConfig, key))),
                        null,
                        leaf,
                        notificationCtx,
                        ConfigurationListener::onUpdate
                );

                return null;
            }

            /** {@inheritDoc} */
            @Override
            public Void visitInnerNode(String key, InnerNode nestedInnerNode) {
                DynamicConfiguration<InnerNode, ?> nestedNodeConfig = dynamicConfig(config, key);

                notificationCtx.addContainer(nestedNodeConfig, null);

                notifyListeners(
                        nestedInnerNode,
                        nestedNodeConfig,
                        viewReadOnly(anyConfigs, anyConfig -> dynamicConfig(anyConfig, key)),
                        notificationCtx
                );

                notificationCtx.removeContainer(nestedNodeConfig);

                return null;
            }

            /** {@inheritDoc} */
            @Override
            public Void visitNamedListNode(String key, NamedListNode<?> newNamedList) {
                notifyPublicListeners(
                        listeners(namedDynamicConfig(config, key)),
                        viewReadOnly(anyConfigs, anyConfig -> listeners(namedDynamicConfig(anyConfig, key))),
                        null,
                        newNamedList,
                        notificationCtx,
                        ConfigurationListener::onUpdate
                );

                // Created only.

                // Lazy initialization.
                Collection<DynamicConfiguration<InnerNode, ?>> newAnyConfigs = null;

                for (String name : newNamedList.namedListKeys()) {
                    DynamicConfiguration<InnerNode, ?> namedNodeConfig =
                            (DynamicConfiguration<InnerNode, ?>) namedDynamicConfig(config, key).getConfig(name);

                    notificationCtx.addContainer(namedNodeConfig, name);

                    InnerNode namedInnerNode = newNamedList.getInnerNode(name);

                    notifyPublicListeners(
                            extendedListeners(namedDynamicConfig(config, key)),
                            viewReadOnly(anyConfigs, anyConfig -> extendedListeners(namedDynamicConfig(anyConfig, key))),
                            null,
                            namedInnerNode.specificNode(),
                            notificationCtx,
                            ConfigurationNamedListListener::onCreate
                    );

                    if (newAnyConfigs == null) {
                        newAnyConfigs = mergeAnyConfigs(
                                viewReadOnly(anyConfigs, anyConfig -> any(namedDynamicConfig(anyConfig, key))),
                                any(namedDynamicConfig(config, key))
                        );
                    }

                    notifyListeners(
                            namedInnerNode,
                            namedNodeConfig,
                            newAnyConfigs,
                            notificationCtx
                    );

                    notificationCtx.removeContainer(namedNodeConfig);
                }

                return null;
            }
        }, true);
    }

    private static <L extends ConfigurationListener<?>> void notifyPublicListeners(
            Collection<? extends L> configListeners,
            Collection<? extends Collection<? extends L>> anyListeners,
            @Nullable Object oldValue,
            @Nullable Object newValue,
            ConfigurationNotificationContext notificationCtx,
            BiFunction<L, ConfigurationNotificationEvent, CompletableFuture<?>> invokeListener
    ) {
        // Lazy set.
        ConfigurationNotificationEvent<?> event = null;

        for (Collection<? extends L> listeners : concat(anyListeners, List.of(configListeners))) {
            if (!listeners.isEmpty()) {
                if (event == null) {
                    event = notificationCtx.createEvent(oldValue, newValue);
                }

                for (L listener : listeners) {
                    try {
                        CompletableFuture<?> future = invokeListener.apply(listener, event);

                        assert future != null : invokeListener;

                        if (future.isCompletedExceptionally() || future.isCancelled() || !future.isDone()) {
                            notificationCtx.futures.add(future);
                        }
                    } catch (Throwable t) {
                        notificationCtx.futures.add(CompletableFuture.failedFuture(t));
                    }
                }
            }
        }
    }
}
