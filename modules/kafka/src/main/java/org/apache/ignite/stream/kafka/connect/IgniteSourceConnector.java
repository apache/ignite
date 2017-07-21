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
import org.apache.kafka.connect.source.SourceConnector;

/**
 * Source connector to manage source tasks that listens to registered Ignite grid events and forward them to Kafka.
 *
 * Note that only cache events are enabled for streaming.
 */
public class IgniteSourceConnector extends SourceConnector {
    /** Source properties. */
    private Map<String, String> configProps;

    /** Expected configurations. */
    private static final ConfigDef CONFIG_DEF = new ConfigDef();

    /** {@inheritDoc} */
    @Override public String version() {
        return AppInfoParser.getVersion();
    }

    /** {@inheritDoc} */
    @Override public void start(Map<String, String> props) {
        try {
            A.notNullOrEmpty(props.get(IgniteSourceConstants.CACHE_NAME), "cache name");
            A.notNullOrEmpty(props.get(IgniteSourceConstants.CACHE_CFG_PATH), "path to cache config file");
            A.notNullOrEmpty(props.get(IgniteSourceConstants.CACHE_EVENTS), "Registered cache events");
            A.notNullOrEmpty(props.get(IgniteSourceConstants.TOPIC_NAMES), "Kafka topics");
        }
        catch (IllegalArgumentException e) {
            throw new ConnectException("Cannot start IgniteSourceConnector due to configuration error", e);
        }

        configProps = props;
    }

    /** {@inheritDoc} */
    @Override public Class<? extends Task> taskClass() {
        return IgniteSourceTask.class;
    }

    /** {@inheritDoc} */
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
