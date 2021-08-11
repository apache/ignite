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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2.reduce0;

/**
 * Task for checking snapshot partitions consistency the same way as {@link VerifyBackupPartitionsTaskV2} does.
 * Since a snapshot partitions already stored apart on disk the is no requirement for a cluster upcoming updates
 * to be hold on.
 */
@GridInternal
public class SnapshotPartitionsVerifyTask extends AbstractSnapshotVerificationTask {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * @param ignite Ignite instance.
     */
    @IgniteInstanceResource
    public void ignite(IgniteEx ignite) {
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override protected ComputeJob makeJob(String name, String constId, Collection<String> groups) {
        return new VisorVerifySnapshotPartitionsJob(name, constId, groups);
    }

    /** {@inheritDoc} */
    @Override public @Nullable SnapshotPartitionsVerifyTaskResult reduce(List<ComputeJobResult> results) throws IgniteException {
        return new SnapshotPartitionsVerifyTaskResult(metas, reduce0(results));
    }
}
