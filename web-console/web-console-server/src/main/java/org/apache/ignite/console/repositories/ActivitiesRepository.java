

package org.apache.ignite.console.repositories;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.dto.ActivityKey;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.internal.util.typedef.F;
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
    private OneToManyIndex<ActivityKey, UUID> activitiesIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public ActivitiesRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        txMgr.registerStarter(() -> {
            activitiesTbl = new Table<>(ignite, "wc_activities");
            activitiesIdx = new OneToManyIndex<>(ignite, "wc_account_activities_idx");
        });
    }
    
    /**
     * GET activity List.
     *
     * @param accId Account ID.
     * @param grp Activity group.
     * @param act Activity action.
     *
     * @return Activity.
     */
    public List<Activity> list(UUID accId, String grp) {
        return txMgr.doInTransaction(() -> {
            // Activity period is the current year and month.
            long date = LocalDate.now().atStartOfDay(UTC).withDayOfMonth(1).toInstant().toEpochMilli();

            ActivityKey activityKey = new ActivityKey(accId, date);

            Set<UUID> ids = activitiesIdx.get(activityKey);

            Collection<Activity> activities = activitiesTbl.loadAll(ids);

            List<Activity> activityList = activities
                .stream()
                .filter(item -> item.getGroup().equals(grp))
                .collect(Collectors.toList())
                ;
           
            return activityList;
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

            Set<UUID> ids = activitiesIdx.get(activityKey);

            Collection<Activity> activities = activitiesTbl.loadAll(ids);

            Activity activity = activities
                .stream()
                .filter(item -> item.getGroup().equals(grp) && item.getAction().equals(act))
                .findFirst()
                .orElse(new Activity(UUID.randomUUID(), accId, grp, act, 0));

            activity.increment(1);

            activitiesTbl.save(activity);            

            activitiesIdx.add(activityKey, activity.getId());

            return activity;
        });
    }
    
    /**
     * delete activity.
     *
     * @param accId Account ID.
     * @param grp Activity group.
     * @param act Activity action.
     *
     * @return Activity.
     */
    public Activity delete(UUID accId, String grp, String act) {
        return txMgr.doInTransaction(() -> {
            // Activity period is the current year and month.
            long date = LocalDate.now().atStartOfDay(UTC).withDayOfMonth(1).toInstant().toEpochMilli();

            ActivityKey activityKey = new ActivityKey(accId, date);

            Set<UUID> ids = activitiesIdx.get(activityKey);

            Collection<Activity> activities = activitiesTbl.loadAll(ids);

            Activity activity = activities
                .stream()
                .filter(item -> item.getGroup().equals(grp) && item.getAction().equals(act))
                .findFirst()
                .orElse(null);
            
            if(activity!=null) {
	            activitiesTbl.delete(activity.getId());
	
	            activitiesIdx.remove(activityKey, activity.getId());
            }

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

                Set<UUID> ids = activitiesIdx.get(key);

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
            Set<UUID> ids = activitiesIdx.get(activityKey);

            activitiesTbl.save(activity);

            ids.add(activity.getId());

            activitiesIdx.addAll(activityKey, ids);
        });
    }
}
