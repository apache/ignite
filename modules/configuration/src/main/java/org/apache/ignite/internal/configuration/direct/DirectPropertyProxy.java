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

package org.apache.ignite.internal.configuration.direct;

import java.util.List;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.internal.configuration.ConfigurationNode;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;

/**
 * Base class for configuration properties that use direct storage access to get the value of {@link #value()}.
 *
 * @see ConfigurationNode#directProxy()
 */
public abstract class DirectPropertyProxy<VIEWT> implements ConfigurationProperty<VIEWT> {
    /** Full path to the current node. */
    protected final List<KeyPathNode> keys;

    /** Name of the current node. Same as last element of {@link #keys}. */
    protected final String key;

    /** Configuration changer instance to get latest value of the root. */
    protected final DynamicConfigurationChanger changer;

    /**
     * Constructor.
     *
     * @param keys Full path to the node.
     * @param changer Changer.
     */
    protected DirectPropertyProxy(
            List<KeyPathNode> keys,
            DynamicConfigurationChanger changer
    ) {
        this.keys = keys;
        this.key = keys.get(keys.size() - 1).key;
        this.changer = changer;
    }

    /** {@inheritDoc} */
    @Override
    public final String key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override
    public final VIEWT value() {
        return changer.getLatest(keys);
    }

    /** {@inheritDoc} */
    @Override
    public final void listen(ConfigurationListener<VIEWT> listener) {
        throw new UnsupportedOperationException("listen");
    }

    /** {@inheritDoc} */
    @Override
    public final void stopListen(ConfigurationListener<VIEWT> listener) {
        throw new UnsupportedOperationException("stopListen");
    }
}
