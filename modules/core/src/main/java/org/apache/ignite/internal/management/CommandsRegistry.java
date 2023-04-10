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

package org.apache.ignite.internal.management;

import org.apache.ignite.internal.management.api.CommandWithSubs;
import org.apache.ignite.internal.management.baseline.BaselineCommand;
import org.apache.ignite.internal.management.encryption.EncryptionCommand;
import org.apache.ignite.internal.management.kill.KillCommand;
import org.apache.ignite.internal.management.meta.MetaCommand;
import org.apache.ignite.internal.management.performancestatistics.PerformanceStatisticsCommand;
import org.apache.ignite.internal.management.persistence.PersistenceCommand;
import org.apache.ignite.internal.management.property.PropertyCommand;
import org.apache.ignite.internal.management.snapshot.SnapshotCommand;
import org.apache.ignite.internal.management.tracing.TracingConfigurationCommand;
import org.apache.ignite.internal.management.wal.WalCommand;

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
