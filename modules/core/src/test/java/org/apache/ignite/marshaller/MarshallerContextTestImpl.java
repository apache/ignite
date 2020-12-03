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

package org.apache.ignite.marshaller;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.Nullable;

/**
 * Test marshaller context.
 */
public class MarshallerContextTestImpl extends MarshallerContextImpl {
    /** */
    private static final ConcurrentMap<Integer, String> map = new ConcurrentHashMap<>();

    /** */
    private final Collection<String> excluded;

    /**
     * Initializes context.
     *
     * @param plugins Plugins.
     * @param excluded Excluded classes.
     */
    public MarshallerContextTestImpl(@Nullable List<PluginProvider> plugins, Collection<String> excluded) {
        super(plugins, null);

        this.excluded = excluded;
    }

    /**
     * Initializes context.
     *
     * @param plugins Plugins.
     */
    public MarshallerContextTestImpl(List<PluginProvider> plugins) {
        this(plugins, null);
    }

    /**
     * Initializes context.
     */
    public MarshallerContextTestImpl() {
        this(null);
    }

    /**
     * @return Internal map.
     */
    public ConcurrentMap<Integer, String> internalMap() {
        return map;
    }

    /** {@inheritDoc} */
    @Override public boolean registerClassName(byte platformId, int typeId, String clsName, boolean failIfUnregistered) throws IgniteCheckedException {
        if (excluded != null && excluded.contains(clsName))
            return false;

        String oldClsName = map.putIfAbsent(typeId, clsName);

        if (oldClsName != null && !oldClsName.equals(clsName))
            throw new IgniteCheckedException("Duplicate ID [id=" + typeId + ", oldClsName=" + oldClsName + ", clsName=" +
                    clsName + ']');

        return true;
    }

    /** {@inheritDoc} */
    @Override public String getClassName(
            byte platformId,
            int typeId
    ) throws ClassNotFoundException, IgniteCheckedException {
        String clsName = map.get(typeId);

        return (clsName == null) ? super.getClassName(platformId, typeId) : clsName;
    }
}
