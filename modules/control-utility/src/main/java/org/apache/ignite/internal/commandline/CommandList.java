/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.commandline.cache.CacheCommands;
import org.apache.ignite.internal.commandline.consistency.ConsistencyCommand;
import org.apache.ignite.internal.commandline.diagnostic.DiagnosticCommand;
import org.apache.ignite.internal.commandline.metric.MetricCommand;
import org.apache.ignite.internal.commandline.snapshot.SnapshotCommand;

/**
 * High-level commands.
 */
public enum CommandList {
    /** */
    ACTIVATE("--activate", new ActivateCommand()),

    /** */
    DEACTIVATE("--deactivate", new DeactivateCommand()),

    /** */
    STATE("--state", new StateCommand()),

    /** */
    SET_STATE("--set-state", new ClusterStateChangeCommand()),

    /** */
    BASELINE("--baseline", new BaselineCommand()),

    /** */
    TX("--tx", new TxCommands()),

    /** */
    CACHE("--cache", new CacheCommands()),

    /** */
    WAL("--wal", new DeclarativeCommandAdapter<>("Wal")),

    /** */
    DIAGNOSTIC("--diagnostic", new DiagnosticCommand()),

    /** Encryption features command. */
    ENCRYPTION("--encryption", new DeclarativeCommandAdapter<>("Encryption")),

    /** Kill command. */
    KILL("--kill", new DeclarativeCommandAdapter<>("Kill")),

    /** Snapshot commands. */
    SNAPSHOT("--snapshot", new SnapshotCommand()),

    /** Change Cluster tag command. */
    CLUSTER_CHANGE_TAG("--change-tag", new DeclarativeCommandAdapter<>("ChangeTag")),

    /** Metadata commands. */
    METADATA("--meta", new DeclarativeCommandAdapter<>("Meta")),

    /** */
    SHUTDOWN_POLICY("--shutdown-policy", new DeclarativeCommandAdapter<>("ShutdownPolicy")),

    /** */
    TRACING_CONFIGURATION("--tracing-configuration", new TracingConfigurationCommand()),

    /** Warm-up command. */
    WARM_UP("--warm-up", new WarmUpCommand()),

    /** Commands to manage distributed properties. */
    PROPERTY("--property", new DeclarativeCommandAdapter<>("Property")),

    /** Command for printing system view content. */
    SYSTEM_VIEW("--system-view", new DeclarativeCommandAdapter<>("SystemView")),

    /** Command for printing metric values. */
    METRIC("--metric", new MetricCommand()),

    /** */
    PERSISTENCE("--persistence", new PersistenceCommand()),

    /** Command to manage PDS defragmentation. */
    DEFRAGMENTATION("--defragmentation", new DefragmentationCommand()),

    /** Command to manage performance statistics. */
    PERFORMANCE_STATISTICS("--performance-statistics", new DeclarativeCommandAdapter<>("PerformanceStatistics")),

    /** Command to check/repair consistency. */
    CONSISTENCY("--consistency", new ConsistencyCommand()),

    /** Cdc commands. */
    CDC("--cdc", new DeclarativeCommandAdapter<>("Cdc"));

    /** Private values copy so there's no need in cloning it every time. */
    private static final CommandList[] VALUES = CommandList.values();

    /** */
    private final String text;

    /** Command implementation. */
    private final Command<?> command;

    /**
     * @param text Text.
     * @param command Command implementation.
     */
    CommandList(String text, Command<?> command) {
        this.text = text;
        this.command = command;
    }

    /**
     * @return Map with commands.
     */
    public static Map<String, Command<?>> commands() {
        return Arrays.stream(VALUES).collect(
            Collectors.toMap(CommandList::text, CommandList::command, (a, b) -> a, LinkedHashMap::new));
    }

    /**
     * @return Command text.
     */
    public String text() {
        return text;
    }

    /**
     * @return Command implementation.
     */
    public Command<?> command() {
        return command;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return text;
    }

    /**
     * @return command name
     */
    public String toCommandName() {
        return text.substring(2).toUpperCase();
    }
}
