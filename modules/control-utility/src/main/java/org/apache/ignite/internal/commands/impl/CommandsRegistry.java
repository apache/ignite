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

package org.apache.ignite.internal.commands.impl;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.internal.commands.ActivateCommand;
import org.apache.ignite.internal.commands.ChangeTagCommand;
import org.apache.ignite.internal.commands.ConsistencyCommand;
import org.apache.ignite.internal.commands.DeactivateCommand;
import org.apache.ignite.internal.commands.DefragmentationCommand;
import org.apache.ignite.internal.commands.DiagnosticCommand;
import org.apache.ignite.internal.commands.MetricCommand;
import org.apache.ignite.internal.commands.SetStateCommand;
import org.apache.ignite.internal.commands.ShutdownPolicyCommand;
import org.apache.ignite.internal.commands.StateCommand;
import org.apache.ignite.internal.commands.SystemViewCommand;
import org.apache.ignite.internal.commands.TxCommand;
import org.apache.ignite.internal.commands.WarmUpCommand;
import org.apache.ignite.internal.commands.api.Command;
import org.apache.ignite.internal.commands.baseline.BaselineCommand;
import org.apache.ignite.internal.commands.encryption.EncryptionCommand;
import org.apache.ignite.internal.commands.kill.KillCommand;
import org.apache.ignite.internal.commands.meta.MetaCommand;
import org.apache.ignite.internal.commands.performancestatistics.PerformanceStatisticsCommand;
import org.apache.ignite.internal.commands.persistence.PersistenceCommand;
import org.apache.ignite.internal.commands.property.PropertyCommand;
import org.apache.ignite.internal.commands.snapshot.SnapshotCommand;
import org.apache.ignite.internal.commands.wal.WalCommand;

/**
 *
 */
public class CommandsRegistry implements Iterable<Command> {
    /** */
    private final Map<String, Command> commands = new LinkedHashMap<>();

    /** */
    public CommandsRegistry() {
        register(new ActivateCommand());
        register(new DeactivateCommand());
        register(new StateCommand());
        register(new SetStateCommand());
        register(new BaselineCommand());
        register(new TxCommand());
        register(new WalCommand());
        register(new DiagnosticCommand());
        register(new EncryptionCommand());
        register(new KillCommand());
        register(new SnapshotCommand());
        register(new ChangeTagCommand());
        register(new MetaCommand());
        register(new ShutdownPolicyCommand());
        register(new WarmUpCommand());
        register(new PropertyCommand());
        register(new SystemViewCommand());
        register(new MetricCommand());
        register(new PersistenceCommand());
        register(new DefragmentationCommand());
        register(new PerformanceStatisticsCommand());
        register(new ConsistencyCommand());
    }

    /** */
    public void register(Command cmd) {
        String name = cmd.getClass().getSimpleName();

        if (!name.endsWith(CommandUtils.CMD_NAME_POSTFIX))
            throw new IllegalArgumentException("Command class name must ends with 'Command'");

        commands.put(CommandUtils.commandName(name.substring(0, name.length() - CommandUtils.CMD_NAME_POSTFIX.length())), cmd);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Command> iterator() {
        return commands.values().iterator();
    }
}
