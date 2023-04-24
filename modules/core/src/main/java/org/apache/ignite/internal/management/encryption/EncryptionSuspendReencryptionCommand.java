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

package org.apache.ignite.internal.management.encryption;

import org.apache.ignite.internal.management.api.AbstractCommand;
import org.apache.ignite.internal.visor.encryption.VisorCacheGroupEncryptionTaskResult;
import org.apache.ignite.internal.visor.encryption.VisorReencryptionSuspendTask;

/** */
public class EncryptionSuspendReencryptionCommand
    extends AbstractCommand<EncryptionCacheGroupArg, VisorCacheGroupEncryptionTaskResult<Boolean>, VisorReencryptionSuspendTask> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Suspend re-encryption of the cache group";
    }

    /** {@inheritDoc} */
    @Override public Class<EncryptionCacheGroupArg> args() {
        return EncryptionCacheGroupArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorReencryptionSuspendTask> task() {
        return VisorReencryptionSuspendTask.class;
    }
}
