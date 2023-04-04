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

import org.apache.ignite.internal.commands.ActivateCommand;
import org.apache.ignite.internal.commands.CdcCommand;
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
import org.apache.ignite.internal.commands.api.CommandWithSubs;
import org.apache.ignite.internal.commands.baseline.BaselineCommand;
import org.apache.ignite.internal.commands.encryption.EncryptionCommand;
import org.apache.ignite.internal.commands.kill.KillCommand;
import org.apache.ignite.internal.commands.meta.MetaCommand;
import org.apache.ignite.internal.commands.performancestatistics.PerformanceStatisticsCommand;
import org.apache.ignite.internal.commands.persistence.PersistenceCommand;
import org.apache.ignite.internal.commands.property.PropertyCommand;
import org.apache.ignite.internal.commands.snapshot.SnapshotCommand;
import org.apache.ignite.internal.commands.tracing.TracingConfigurationCommand;
import org.apache.ignite.internal.commands.wal.WalCommand;

/**
 *
 */
public class CommandsRegistry extends CommandWithSubs {
    /** */
    public CommandsRegistry() {
        register(ActivateCommand::new);
        register(DeactivateCommand::new);
        register(StateCommand::new);
        register(SetStateCommand::new);
        register(BaselineCommand::new);
        register(TxCommand::new);
        register(CacheCommand::new);
        register(WalCommand::new);
        register(DiagnosticCommand::new);
        register(EncryptionCommand::new);
        register(KillCommand::new);
        register(SnapshotCommand::new);
        register(ChangeTagCommand::new);
        register(MetaCommand::new);
        register(ShutdownPolicyCommand::new);
        register(TracingConfigurationCommand::new);
        register(WarmUpCommand::new);
        register(PropertyCommand::new);
        register(SystemViewCommand::new);
        register(MetricCommand::new);
        register(PersistenceCommand::new);
        register(DefragmentationCommand::new);
        register(PerformanceStatisticsCommand::new);
        register(ConsistencyCommand::new);
        register(CdcCommand::new);
    }
}
