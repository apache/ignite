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
import java.util.ArrayList;
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
import org.apache.ignite.console.dto.DBInfoDto;
import org.apache.ignite.console.dto.TaskFlow;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.internal.util.typedef.F;
import org.springframework.stereotype.Repository;

import static java.time.ZoneOffset.UTC;

/**
 * Repository to work with activities.
 */
@Repository
public class TaskFlowRepository {
    /** */
    private final TransactionManager txMgr;

    /** */
    private Table<TaskFlow> taskflowTbl;

    /** */
    private OneToManyIndex<UUID, UUID> accountsIdx;
    
    /** */
    private OneToManyIndex<String, UUID> grpIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public TaskFlowRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        txMgr.registerStarter(() -> {
            taskflowTbl = new Table<>(ignite, "wc_task_flow");
            accountsIdx = new OneToManyIndex<>(ignite, "wc_account_task_flow_idx");
            grpIdx = new OneToManyIndex<>(ignite, "wc_grp_task_flow_idx");
        });
    }

    /**
     * @param accId Account ID.
     * @return List of notebooks for specified account.
     */
    public Collection<TaskFlow> list(UUID accId) {
        return txMgr.doInTransaction(() -> {
            Set<UUID> notebooksIds = accountsIdx.get(accId);

            return taskflowTbl.loadAll(notebooksIds);
        });
    }
    
    /**
     * @param accId Account ID.
     * @return List of notebooks for specified account.
     */
    public TaskFlow get(UUID accId,UUID flowId) {
        return txMgr.doInTransaction(() -> {
            return taskflowTbl.get(flowId);
        });
    }
    
    /**
     * find activity.
     *
     * @param accId Account ID.
     * @param grp TaskFlow group.
     * @param act TaskFlow action.
     *
     * @return TaskFlow.
     */
    public Collection<TaskFlow> taskFlowForGroup(UUID accId, String grp, String act,String target,String source) {
        return txMgr.doInTransaction(() -> {
            Set<UUID> ids = grpIdx.get(grp);

            Collection<TaskFlow> activities = taskflowTbl.loadAll(ids);
            if(act!=null) {
            	Collection<TaskFlow> activitie2 = activities
                    .stream()
                    .filter(item -> item.getGroup().equals(grp) && item.getAction().equals(act))
                    .collect(Collectors.toList())
                   ;
            	activities  = activitie2;
            }
            if(target!=null) {
            	Collection<TaskFlow> activitie2 = activities
                    .stream()
                    .filter(item -> item.getGroup().equals(grp) && item.getTarget().equals(target))
                    .collect(Collectors.toList())
                   ;
            	activities = activitie2;
            }
            if(source!=null) {
            	Collection<TaskFlow> activitie2 = activities
                    .stream()
                    .filter(item -> item.getGroup().equals(grp) && item.getSource().equals(source))
                    .collect(Collectors.toList())
                   ;
            	activities = activitie2;
            }
            return activities;
        });
    }

    /**
     * @param accId Account ID.
     * @param startDate Start date.
     * @param endDate End date.
     */
    public List<TaskFlow> taskFlowForPeriod(UUID accId, long startDate, long endDate) {
        return txMgr.doInTransaction(() -> {
            ZonedDateTime dtStart = Instant.ofEpochMilli(startDate).atZone(UTC);
            ZonedDateTime dtEnd = Instant.ofEpochMilli(endDate).atZone(UTC);

            List<TaskFlow> totals = new ArrayList<>();

            if (dtStart.isBefore(dtEnd)) {

                Set<UUID> ids = accountsIdx.get(accId);

                if (!F.isEmpty(ids)) {
                    Collection<TaskFlow> activities = taskflowTbl.loadAll(ids);

                    activities.forEach(activity -> {
                        if(activity.getUpdated()>=startDate && activity.getUpdated()<endDate) {
                        	totals.add(activity);
                        }
                    });
                }

                dtStart = dtStart.plusMonths(1);
            }

            return totals;
        });
    }

    /**
     * @param activityKey TaskFlow key.
     * @param activity TaskFlow to save.
     */
    public void save(UUID accId, TaskFlow activity) {
        txMgr.doInTransaction(() -> {
        	accountsIdx.validateBeforeSave(accId, activity.getId(), taskflowTbl);
            taskflowTbl.save(activity);

            accountsIdx.add(accId, activity.getId());
            grpIdx.add(activity.getGroup(), activity.getId());
        });
    }
    
    /**
     * Delete flow.
     *
     * @param accId Account ID.
     * @param flowId Notebook ID to delete.
     */
    public void delete(UUID accId, String grp) {
        txMgr.doInTransaction(() -> {        	
        	Set<UUID> flowId = grpIdx.get(grp);
        	taskflowTbl.deleteAll(flowId);
            accountsIdx.removeAll(accId, flowId);  
            grpIdx.delete(grp);
        });
    }
    
    /**
     * Delete flow.
     *
     * @param accId Account ID.
     * @param flowId Notebook ID to delete.
     */
    public void delete(UUID accId, UUID flowId) {
        txMgr.doInTransaction(() -> {
        	accountsIdx.validate(accId, flowId);

        	TaskFlow flow = taskflowTbl.get(flowId);

            if (flow != null) {
            	taskflowTbl.delete(flowId);

                accountsIdx.remove(accId, flowId);
                grpIdx.remove(flow.getGroup(), flowId);
            }
        });
    }

    /**
     * Delete all flow for specified account.
     *
     * @param accId Account ID.
     */
    public void deleteAll(UUID accId) {
        txMgr.doInTransaction(() -> {
            Set<UUID> flowsIds = accountsIdx.delete(accId);

            taskflowTbl.deleteAll(flowsIds);
            // todo@byron
            //grpIdx.delete(grp);
        });
    }
}
