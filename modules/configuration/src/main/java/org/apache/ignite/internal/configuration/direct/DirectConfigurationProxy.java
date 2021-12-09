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
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;

/**
 * {@link DirectPropertyProxy} implementation for leaves.
 */
public abstract class DirectConfigurationProxy<VIEWT, CHANGET extends VIEWT>
        extends DirectPropertyProxy<VIEWT>
        implements ConfigurationTree<VIEWT, CHANGET> {

    /**
     * Constructor.
     *
     * @param keys Full path to the node.
     * @param changer Changer.
     */
    protected DirectConfigurationProxy(List<KeyPathNode> keys, DynamicConfigurationChanger changer) {
        super(keys, changer);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> change(Consumer<CHANGET> change) {
        throw new UnsupportedOperationException("change");
    }
}
