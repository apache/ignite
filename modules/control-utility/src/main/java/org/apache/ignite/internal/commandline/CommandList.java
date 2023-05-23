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
import org.apache.ignite.internal.commandline.snapshot.SnapshotCommand;
import org.apache.ignite.internal.management.ActivateCommand;
import org.apache.ignite.internal.management.ChangeTagCommand;
import org.apache.ignite.internal.management.DeactivateCommand;
import org.apache.ignite.internal.management.SetStateCommand;
import org.apache.ignite.internal.management.ShutdownPolicyCommand;
import org.apache.ignite.internal.management.StateCommand;
import org.apache.ignite.internal.management.SystemViewCommand;
import org.apache.ignite.internal.management.WarmUpCommand;
import org.apache.ignite.internal.management.baseline.BaselineCommand;
import org.apache.ignite.internal.management.cdc.CdcCommand;
import org.apache.ignite.internal.management.defragmentation.DefragmentationCommand;
import org.apache.ignite.internal.management.diagnostic.DiagnosticCommand;
import org.apache.ignite.internal.management.encryption.EncryptionCommand;
import org.apache.ignite.internal.management.kill.KillCommand;
import org.apache.ignite.internal.management.meta.MetaCommand;
import org.apache.ignite.internal.management.metric.MetricCommand;
import org.apache.ignite.internal.management.performancestatistics.PerformanceStatisticsCommand;
import org.apache.ignite.internal.management.persistence.PersistenceCommand;
import org.apache.ignite.internal.management.property.PropertyCommand;
import org.apache.ignite.internal.management.tracing.TracingConfigurationCommand;
import org.apache.ignite.internal.management.tx.TxCommand;
import org.apache.ignite.internal.management.wal.WalCommand;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAMETER_PREFIX;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedCommandName;

/**
 * High-level commands.
 */
public enum CommandList {
    /** */
    ACTIVATE(new ActivateCommand()),

    /** */
    DEACTIVATE(new DeactivateCommand()),

    /** */
    STATE(new StateCommand()),

    /** */
    SET_STATE(new SetStateCommand()),

    /** */
    BASELINE(new BaselineCommand()),

    /** */
    TX(new TxCommand()),

    /** */
    CACHE("--cache", new CacheCommands()),

    /** */
    WAL(new WalCommand()),

    /** */
    DIAGNOSTIC(new DiagnosticCommand()),

    /** Encryption features command. */
    ENCRYPTION(new EncryptionCommand()),

    /** Kill command. */
    KILL(new KillCommand()),

    /** Snapshot commands. */
    SNAPSHOT("--snapshot", new SnapshotCommand()),

    /** Change Cluster tag command. */
    CLUSTER_CHANGE_TAG(new ChangeTagCommand()),

    /** Metadata commands. */
    METADATA(new MetaCommand()),

    /** */
    SHUTDOWN_POLICY(new ShutdownPolicyCommand()),

    /** */
    TRACING_CONFIGURATION(new TracingConfigurationCommand()),

    /** Warm-up command. */
    WARM_UP(new WarmUpCommand()),

    /** Commands to manage distributed properties. */
    PROPERTY(new PropertyCommand()),

    /** Command for printing system view content. */
    SYSTEM_VIEW(new SystemViewCommand()),

    /** Command for printing metric values. */
    METRIC(new MetricCommand()),

    /** */
    PERSISTENCE(new PersistenceCommand()),

    /** Command to manage PDS defragmentation. */
    DEFRAGMENTATION(new DefragmentationCommand()),

    /** Command to manage performance statistics. */
    PERFORMANCE_STATISTICS(new PerformanceStatisticsCommand()),

    /** Command to check/repair consistency. */
    CONSISTENCY("--consistency", new ConsistencyCommand()),

    /** Cdc commands. */
    CDC(new CdcCommand());

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

    /** @param command Management API command. */
    CommandList(org.apache.ignite.internal.management.api.Command<?, ?> command) {
        this.text = PARAMETER_PREFIX + toFormattedCommandName(command.getClass());
        this.command = new DeclarativeCommandAdapter<>(command);
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
