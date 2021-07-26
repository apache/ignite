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

package org.apache.ignite.internal.configuration.hocon;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static org.apache.ignite.internal.configuration.hocon.HoconPrimitiveConfigurationSource.wrongTypeException;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.appendKey;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.join;

/**
 * {@link ConfigurationSource} created from a HOCON object.
 */
class HoconObjectConfigurationSource implements ConfigurationSource {
    /**
     * Key that needs to be ignored by the source. Can be {@code null}.
     */
    private final String ignoredKey;

    /**
     * Current path inside the top-level HOCON object.
     */
    private final List<String> path;

    /**
     * HOCON object that this source has been created from.
     */
    private final ConfigObject hoconCfgObject;

    /**
     * Creates a {@link ConfigurationSource} from the given HOCON object.
     *
     * @param ignoredKey Key that needs to be ignored by the source. Can be {@code null}.
     * @param path Current path inside the top-level HOCON object. Can be empty if the given {@code hoconCfgObject}
     *             is the top-level object.
     * @param hoconCfgObject HOCON object.
     */
    HoconObjectConfigurationSource(@Nullable String ignoredKey, List<String> path, ConfigObject hoconCfgObject) {
        this.ignoredKey = ignoredKey;
        this.path = path;
        this.hoconCfgObject = hoconCfgObject;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        throw wrongTypeException(clazz, path, -1);
    }

    /** {@inheritDoc} */
    @Override public void descend(ConstructableTreeNode node) {
        for (Map.Entry<String, ConfigValue> entry : hoconCfgObject.entrySet()) {
            String key = entry.getKey();

            if (key.equals(ignoredKey))
                continue;

            ConfigValue hoconCfgValue = entry.getValue();

            try {
                switch (hoconCfgValue.valueType()) {
                    case NULL:
                        node.construct(key, null);

                        break;

                    case OBJECT: {
                        List<String> path = appendKey(this.path, key);

                        node.construct(key, new HoconObjectConfigurationSource(null, path, (ConfigObject)hoconCfgValue));

                        break;
                    }

                    case LIST: {
                        List<String> path = appendKey(this.path, key);

                        node.construct(key, new HoconListConfigurationSource(path, (ConfigList)hoconCfgValue));

                        break;
                    }

                    default: {
                        List<String> path = appendKey(this.path, key);

                        node.construct(key, new HoconPrimitiveConfigurationSource(path, hoconCfgValue));
                    }
                }
            }
            catch (NoSuchElementException e) {
                if (path.isEmpty()) {
                    throw new IllegalArgumentException(
                        format("'%s' configuration root doesn't exist", key), e
                    );
                }
                else {
                    throw new IllegalArgumentException(
                        format("'%s' configuration doesn't have the '%s' sub-configuration", join(path), key), e
                    );
                }
            }
        }
    }
}
