/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.services;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.repositories.ActivitiesRepository;
import org.springframework.stereotype.Service;

/**
 * Service to handle activities.
 */
@Service
public class ActivitiesService {
    /** */
    private final ActivitiesRepository activitiesRepo;

    /**
     * @param activitiesRepo Repository to work with activities.
     */
    public ActivitiesService(ActivitiesRepository activitiesRepo) {
        this.activitiesRepo = activitiesRepo;
    }

    /**
     * @param accId Account ID.
     * @param grp Activity group.
     * @param act Activity action.
     */
    public void save(UUID accId, String grp, String act) {
        activitiesRepo.save(accId, grp, act);
    }

    /**
     * @param accId Account ID.
     * @param startDate Start date.
     * @param endDate End date.
     * @return Collection of user activities.
     */
    public Collection<Activity> activitiesForPeriod(UUID accId, long startDate, long endDate) {
        return activitiesRepo.activitiesForPeriod(accId, startDate, endDate);
    }
}
