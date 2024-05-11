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

package org.apache.ignite.stream.kafka.connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * Sink connector to manage sink tasks that transfer Kafka data to Ignite grid.
 */
public class IgniteSinkConnector extends SinkConnector {
    /** Sink properties. */
    private Map<String, String> configProps;

    /** Expected configurations. */
    private static final ConfigDef CONFIG_DEF = new ConfigDef();

    /** {@inheritDoc} */
    @Override public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * A sink lifecycle method. Validates grid-specific sink properties.
     *
     * @param props Sink properties.
     */
    @Override public void start(Map<String, String> props) {
        configProps = props;

        try {
            A.notNullOrEmpty(configProps.get(SinkConnector.TOPICS_CONFIG), "topics");
            A.notNullOrEmpty(configProps.get(IgniteSinkConstants.CACHE_NAME), "cache name");
            A.notNullOrEmpty(configProps.get(IgniteSinkConstants.CACHE_CFG_PATH), "path to cache config file");
        }
        catch (IllegalArgumentException e) {
            throw new ConnectException("Cannot start IgniteSinkConnector due to configuration error", e);
        }
    }

    /**
     * Obtains a sink task class to be instantiated for feeding data into grid.
     *
     * @return IgniteSinkTask class.
     */
    @Override public Class<? extends Task> taskClass() {
        return IgniteSinkTask.class;
    }

    /**
     * Builds each config for <tt>maxTasks</tt> tasks.
     *
     * @param maxTasks Max number of tasks.
     * @return Task configs.
     */
    @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();

        taskProps.putAll(configProps);

        for (int i = 0; i < maxTasks; i++)
            taskConfigs.add(taskProps);

        return taskConfigs;
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public ConfigDef config() {
        return CONFIG_DEF;
    }
}
