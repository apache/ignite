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

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallerContextAdapter;
import org.apache.ignite.plugin.PluginProvider;
import org.jsr166.ConcurrentHashMap8;

/**
 * Test marshaller context.
 */
public class MarshallerContextTestImpl extends MarshallerContextAdapter {
    /** */
    private final static ConcurrentMap<Integer, String> map = new ConcurrentHashMap8<>();

    /**
     * Initializes context.
     *
     * @param plugins Plugins.
     */
    public MarshallerContextTestImpl(List<PluginProvider> plugins) {
        super(plugins);
    }

    /**
     * Initializes context.
     */
    public MarshallerContextTestImpl() {
        super(null);
    }

    /** {@inheritDoc} */
    @Override protected boolean registerClassName(int id, String clsName) throws IgniteCheckedException {
        String oldClsName = map.putIfAbsent(id, clsName);

        if (oldClsName != null && !oldClsName.equals(clsName))
            throw new IgniteCheckedException("Duplicate ID [id=" + id + ", oldClsName=" + oldClsName + ", clsName=" +
                clsName + ']');

        return true;
    }

    /** {@inheritDoc} */
    @Override protected String className(int id) {
        return map.get(id);
    }
}