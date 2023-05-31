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

import org.apache.ignite.internal.management.api.CommandRegistryImpl;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.baseline.BaselineCommand;
import org.apache.ignite.internal.management.cache.CacheCommand;
import org.apache.ignite.internal.management.cdc.CdcCommand;
import org.apache.ignite.internal.management.consistency.ConsistencyCommand;
import org.apache.ignite.internal.management.defragmentation.DefragmentationCommand;
import org.apache.ignite.internal.management.diagnostic.DiagnosticCommand;
import org.apache.ignite.internal.management.encryption.EncryptionCommand;
import org.apache.ignite.internal.management.kill.KillCommand;
import org.apache.ignite.internal.management.meta.MetaCommand;
import org.apache.ignite.internal.management.metric.MetricCommand;
import org.apache.ignite.internal.management.performancestatistics.PerformanceStatisticsCommand;
import org.apache.ignite.internal.management.persistence.PersistenceCommand;
import org.apache.ignite.internal.management.property.PropertyCommand;
import org.apache.ignite.internal.management.snapshot.SnapshotCommand;
import org.apache.ignite.internal.management.tracing.TracingConfigurationCommand;
import org.apache.ignite.internal.management.tx.TxCommand;
import org.apache.ignite.internal.management.wal.WalCommand;

/**
 * Ignite command registry containing all commands known by Ignite node.
 *
 * @see org.apache.ignite.internal.management.api.Command
 */
public class IgniteCommandRegistry extends CommandRegistryImpl<NoArg, Void> {
    /** */
    public IgniteCommandRegistry() {
        super(
            new ActivateCommand(),
            new DeactivateCommand(),
            new StateCommand(),
            new SetStateCommand(),
            new BaselineCommand(),
            new TxCommand(),
            new CacheCommand(),
            new WalCommand(),
            new DiagnosticCommand(),
            new EncryptionCommand(),
            new KillCommand(),
            new SnapshotCommand(),
            new ChangeTagCommand(),
            new MetaCommand(),
            new ShutdownPolicyCommand(),
            new TracingConfigurationCommand(),
            new WarmUpCommand(),
            new PropertyCommand(),
            new SystemViewCommand(),
            new MetricCommand(),
            new PersistenceCommand(),
            new DefragmentationCommand(),
            new PerformanceStatisticsCommand(),
            new ConsistencyCommand(),
            new CdcCommand()
        );
    }
}
