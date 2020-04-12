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

package org.apache.ignite.internal.processors.configuration.distributed;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import static org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationProcessor.AllowableAction.ACTUALIZE;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationProcessor.AllowableAction.CLUSTER_WIDE_UPDATE;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationProcessor.AllowableAction.REGISTER;

/**
 * Processor of distributed configuration.
 *
 * This class control lifecycle of actualization {@link DistributedProperty} across whole cluster.
 */
public class DistributedConfigurationProcessor extends GridProcessorAdapter implements DistributedPropertyDispatcher {
    /** Prefix of key for distributed meta storage. */
    private static final String DIST_CONF_PREFIX = "distrConf-";

    /** Properties storage. */
    private final Map<String, DistributedChangeableProperty> props = new ConcurrentHashMap<>();

    /** Global metastorage. */
    private volatile DistributedMetaStorage distributedMetastorage;

    /** Max allowed action. All action with less ordinal than this also allowed. */
    private volatile AllowableAction allowableAction = REGISTER;

    /**
     * @param ctx Kernal context.
     */
    public DistributedConfigurationProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        GridInternalSubscriptionProcessor isp = ctx.internalSubscriptionProcessor();

        isp.registerDistributedMetastorageListener(new DistributedMetastorageLifecycleListener() {
            @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                distributedMetastorage = ctx.distributedMetastorage();

                //Listener for handling of cluster wide change of specific properties. Do local update.
                distributedMetastorage.listen(
                    (key) -> key.startsWith(DIST_CONF_PREFIX),
                    (String key, Serializable oldVal, Serializable newVal) -> {
                        DistributedChangeableProperty prop = props.get(toPropertyKey(key));

                        if (prop != null)
                            prop.localUpdate(newVal);
                    }
                );

                //Switch to actualize action and actualize already registered properties.
                switchCurrentActionTo(ACTUALIZE);

                //Register and actualize properties waited for this service.
                isp.getDistributedConfigurationListeners()
                    .forEach(listener -> listener.onReadyToRegister(DistributedConfigurationProcessor.this));

            }

            @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                //Switch to cluster wide update action and do it on already registered properties.
                switchCurrentActionTo(CLUSTER_WIDE_UPDATE);

                isp.getDistributedConfigurationListeners()
                    .forEach(DistributedConfigurationLifecycleListener::onReadyToWrite);
            }
        });
    }

    /**
     * Switching current action to given action and do all actions from old action to new one.
     *
     * @param to New action for switching on.
     */
    private synchronized void switchCurrentActionTo(AllowableAction to) {
        AllowableAction oldAct = allowableAction;

        assert oldAct.ordinal() <= to.ordinal() : "Current action : " + oldAct + ", new action : " + to;

        allowableAction = to;

        for (AllowableAction action : AllowableAction.values()) {
            if (action.ordinal() > oldAct.ordinal())
                props.values().forEach(prop -> doAction(action, prop));

            if (action == to)
                break;
        }
    }

    /**
     * @param propKey Key of specific property.
     * @return Property key for meta storage.
     */
    private static String toMetaStorageKey(String propKey) {
        return DIST_CONF_PREFIX + propKey;
    }

    /**
     * @param metaStorageKey Key from meta storage.
     * @return Original property key.
     */
    private static String toPropertyKey(String metaStorageKey) {
        return metaStorageKey.substring(DIST_CONF_PREFIX.length());
    }

    /** {@inheritDoc} */
    @Override public <T extends DistributedChangeableProperty> void registerProperties(T... props) {
        Arrays.stream(props)
            .forEach(this::registerProperty);
    }

    /**
     * Register property to processor and attach it if it possible.
     *
     * @param prop Property to attach to processor.
     */
    @Override public <T extends Serializable> DistributedProperty<T> registerProperty(
        DistributedChangeableProperty<T> prop
    ) {
        doAllAllowableActions(prop);

        return prop;
    }

    /**
     * Execute all allowable actions until current action on given property.
     *
     * @param prop Property which action should be executed on.
     */
    private void doAllAllowableActions(DistributedChangeableProperty prop) {
        for (AllowableAction action : AllowableAction.values()) {
            doAction(action, prop);

            if (action == allowableAction)
                break;
        }
    }

    /**
     * Do one given action on given property.
     *
     * @param act Action to execute.
     * @param prop Property which action should be execute on.
     */
    private void doAction(AllowableAction act, DistributedChangeableProperty prop) {
        switch (act) {
            case REGISTER:
                doRegister(prop);
                break;
            case ACTUALIZE:
                doActualize(prop);
                break;
            case CLUSTER_WIDE_UPDATE:
                doClusterWideUpdate(prop);
                break;
        }
    }

    /**
     * Do register action on given property.
     *
     * Bind property with this processor for furthter actualizing.
     *
     * @param prop Property which action should be execute on.
     */
    private void doRegister(DistributedChangeableProperty prop) {
        if (props.containsKey(prop.getName()))
            throw new IllegalArgumentException("Property already exists : " + prop.getName());

        props.put(prop.getName(), prop);

        prop.onAttached();
    }

    /**
     * Do actualize action on given property.
     *
     * Read actual value from metastore and set it to local property.
     *
     * @param prop Property which action should be execute on.
     */
    private void doActualize(DistributedChangeableProperty prop) {
        Serializable readVal = null;
        try {
            readVal = distributedMetastorage.read(toMetaStorageKey(prop.getName()));
        }
        catch (IgniteCheckedException e) {
            log.error("Can not read value of property '" + prop.getName() + "'", e);
        }

        prop.localUpdate(readVal);
    }

    /**
     * Do cluster wide action on given property.
     *
     * Set closure for cluster wide update action to given property.
     *
     * @param prop Property which action should be execute on.
     */
    private void doClusterWideUpdate(DistributedChangeableProperty prop) {
        prop.onReadyForUpdate(new PropertyUpdateClosure() {
            @Override public GridFutureAdapter<?> update(String key, Serializable newValue)
                throws IgniteCheckedException {
                return distributedMetastorage.writeAsync(toMetaStorageKey(key), newValue);
            }

            @Override public GridFutureAdapter<?> casUpdate(
                String key,
                Serializable expectedValue,
                Serializable newValue
            ) throws IgniteCheckedException {
                return distributedMetastorage.compareAndSetAsync(toMetaStorageKey(key), expectedValue, newValue);
            }
        });
    }

    /**
     * This enum determinate what is action allowable for proccessor in current moment.
     *
     * Order is important. Each next action allowable all previous actions. Current action can be changed only from
     * previous to next .
     */
    enum AllowableAction {
        /**
         * Only registration allowed. Actualization property from metastore and cluster wide update aren't allowed.
         */
        REGISTER,
        /**
         * Registration and actualization property from metastore are allowed. Cluster wide update isn't allowed.
         */
        ACTUALIZE,
        /**
         * All of below are allowed.
         */
        CLUSTER_WIDE_UPDATE;
    }
}
