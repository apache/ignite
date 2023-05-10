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

package org.apache.ignite.internal.management.persistence;

import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.visor.VisorTaskArgument;

/** */
public class PersistenceBackupCorruptedCommand implements ComputeCommand<NoArg, NoArg> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Backup data files of corrupted caches only";
    }

    /** {@inheritDoc} */
    @Override public Class<NoArg> argClass() {
        return NoArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<? extends ComputeTask<VisorTaskArgument<NoArg>, NoArg>> taskClass() {
        return null;
    }
}
