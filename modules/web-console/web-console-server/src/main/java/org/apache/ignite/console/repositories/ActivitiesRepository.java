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

package org.apache.ignite.console.repositories;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.dto.ActivityKey;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.internal.util.typedef.F;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.stereotype.Repository;

import static java.time.ZoneOffset.UTC;

/**
 * Repository to work with activities.
 */
@Repository
public class ActivitiesRepository {
    /** */
    private final TransactionManager txMgr;

    /** */
    private Table<Activity> activitiesTbl;

    /** */
    private OneToManyIndex<ActivityKey> activitiesIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public ActivitiesRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        txMgr.registerStarter("activities", () -> {
            MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

            activitiesTbl = new Table<>(ignite, "wc_activities");

            activitiesIdx = new OneToManyIndex<>(
                    ignite,
                    "wc_account_activities_idx",
                    (key) -> messages.getMessage("err.data-access-violation")
            );
        });
    }

    /**
     * Save activity.
     *
     * @param accId Account ID.
     * @param grp Activity group.
     * @param act Activity action.
     *
     * @return Activity.
     */
    public Activity save(UUID accId, String grp, String act) {
        return txMgr.doInTransaction(() -> {
            // Activity period is the current year and month.
            long date = LocalDate.now().atStartOfDay(UTC).withDayOfMonth(1).toInstant().toEpochMilli();

            ActivityKey activityKey = new ActivityKey(accId, date);

            Set<UUID> ids = activitiesIdx.load(activityKey);

            Collection<Activity> activities = activitiesTbl.loadAll(ids);

            Activity activity = activities
                .stream()
                .filter(item -> item.getGroup().equals(grp) && item.getAction().equals(act))
                .findFirst()
                .orElse(new Activity(UUID.randomUUID(), accId, grp, act, 0));

            activity.increment(1);

            activitiesTbl.save(activity);

            ids.add(activity.getId());

            activitiesIdx.addAll(activityKey, ids);

            return activity;
        });
    }

    /**
     * @param accId Account ID.
     * @param startDate Start date.
     * @param endDate End date.
     */
    public Collection<Activity> activitiesForPeriod(UUID accId, long startDate, long endDate) {
        return txMgr.doInTransaction(() -> {
            ZonedDateTime dtStart = Instant.ofEpochMilli(startDate).atZone(UTC);
            ZonedDateTime dtEnd = Instant.ofEpochMilli(endDate).atZone(UTC);

            Map<String, Activity> totals = new HashMap<>();

            while (dtStart.isBefore(dtEnd)) {
                ActivityKey key = new ActivityKey(accId, dtStart.toInstant().toEpochMilli());

                Set<UUID> ids = activitiesIdx.load(key);

                if (!F.isEmpty(ids)) {
                    Collection<Activity> activities = activitiesTbl.loadAll(ids);

                    activities.forEach(activity -> {
                        Activity total = totals.get(activity.getAction());

                        if (total != null)
                            total.increment(activity.getAmount());
                        else
                            totals.put(activity.getAction(), activity);
                    });
                }

                dtStart = dtStart.plusMonths(1);
            }

            return totals.values();
        });
    }

    /**
     * @param activityKey Activity key.
     * @param activity Activity to save.
     */
    public void save(ActivityKey activityKey, Activity activity) {
        txMgr.doInTransaction(() -> {
            Set<UUID> ids = activitiesIdx.load(activityKey);

            activitiesTbl.save(activity);

            ids.add(activity.getId());

            activitiesIdx.addAll(activityKey, ids);
        });
    }
}
