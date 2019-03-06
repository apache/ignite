/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
