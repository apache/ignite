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

package org.apache.ignite.internal.management.cache;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Task to verify checksums of backup partitions.
 */
@GridInternal
public class IdleVerifyTask extends VisorOneNodeTask<CacheIdleVerifyCommandArg, IdleVerifyResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<CacheIdleVerifyCommandArg, IdleVerifyResult> job(CacheIdleVerifyCommandArg arg) {
        if (!ignite.context().state().publicApiActiveState(true))
            throw new IgniteException(VerifyBackupPartitionsTask.IDLE_VERIFY_ON_INACTIVE_CLUSTER_ERROR_MESSAGE);

        return new IdleVerifyJob<>(arg, debug, VerifyBackupPartitionsTask.class);
    }
}
