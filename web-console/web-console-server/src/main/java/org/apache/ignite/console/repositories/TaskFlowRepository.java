

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
     * @param grp TaskFlow group == targetCluster
     * @param sourceCluster TaskFlow source cluster.
     *
     * @return TaskFlow.
     */
    public Collection<TaskFlow> taskFlowForGroup(UUID accId, String grp, String sourceCluster,String target,String source) {
        return txMgr.doInTransaction(() -> {
            Set<UUID> ids = grpIdx.get(grp);

            Collection<TaskFlow> activities = taskflowTbl.loadAll(ids);
            if(sourceCluster!=null) {
            	Collection<TaskFlow> activitie2 = activities
                    .stream()
                    .filter(item -> item.getGroup().equals(grp) && item.getSourceCluster().equals(sourceCluster))
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
     * @param accId Account ID.    
     */
    public List<TaskFlow> taskFlowForCache(UUID accId, String targetCluster, String action, String target) {
        return txMgr.doInTransaction(() -> {
            List<TaskFlow> totals = new ArrayList<>();
            if (target!=null) {
            	Set<UUID> ids = accountsIdx.get(accId);

                if (!F.isEmpty(ids)) {
                    Collection<TaskFlow> activities = taskflowTbl.loadAll(ids);

                    activities.forEach(activity -> {
                        if(activity.getTargetCluster().equals(targetCluster) && activity.getTarget().equals(target)) {
                        	if(action==null || activity.getAction().equals(action)) {
                        		totals.add(activity);
                        	}
                        }
                    });
                }
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
        	TaskFlow old = taskflowTbl.save(activity);
        	if(old==null) {
        		accountsIdx.add(accId, activity.getId());
        		grpIdx.add(activity.getGroup(), activity.getId());
        	}
        	else if(!old.getId().equals(activity.getId())){
        		accountsIdx.remove(accId, old.getId());
        		grpIdx.remove(old.getGroup(), old.getId());
        		
        		accountsIdx.add(accId, activity.getId());
        		grpIdx.add(activity.getGroup(), activity.getId());
        	}
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

            Collection<TaskFlow> flows = taskflowTbl.loadAll(flowsIds);
            for(TaskFlow flow: flows) {            	
                grpIdx.remove(flow.getGroup(),flow.getId());
            }            
            taskflowTbl.deleteAll(flowsIds);           
            
            
        });
    }
}
