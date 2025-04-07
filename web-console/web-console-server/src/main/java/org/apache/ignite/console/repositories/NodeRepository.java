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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.tx.TransactionManager;
import org.springframework.stereotype.Repository;

/**
 * Repository to work with node activities.
 */
@Repository
public class NodeRepository {
    /** */
    private final TransactionManager txMgr;

    /** */
    private Table<Activity> activitiesTbl;

    /** acc_id -> node_id */
    private OneToManyIndex<UUID, UUID> activitiesIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public NodeRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        txMgr.registerStarter(() -> {
            activitiesTbl = new Table<>(ignite, "wc_nodes");
            activitiesIdx = new OneToManyIndex<>(ignite, "wc_account_nodes_idx");
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
            
            Set<UUID> ids = activitiesIdx.get(accId);

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
     * @param grp Node group.
     * @param act Node adressses.
     *
     * @return Activity.
     */
    public Activity save(UUID accId, UUID nodeId, String grp, String act) {
        return txMgr.doInTransaction(() -> {            

        	Activity activity = activitiesTbl.get(nodeId);
        	if(activity==null)
            	activity = new Activity(nodeId, accId, grp, act, 1);
        	else
        		activity.increment(1);

            activitiesTbl.save(activity);

            activitiesIdx.add(accId, nodeId);

            return activity;
        });
    }
    
    /**
     * clear activity.
     *
     * @param accId Account ID.     
     *
     * @return Activity.
     */
    public void clear() {
    	activitiesIdx.cache().clear();
        activitiesTbl.cache().clear();
    }
    
    /**
     * delete activity.
     *
     * @param accId Account ID.     
     *
     * @return Activity.
     */
    public Activity delete(UUID accId, UUID nodeId) {
        return txMgr.doInTransaction(() -> {
            activitiesIdx.remove(accId,nodeId);

            return activitiesTbl.delete(nodeId);
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
            
            Set<UUID> ids = activitiesIdx.get(accId);

            Collection<Activity> activities = activitiesTbl.loadAll(ids);

            Activity activity = activities
                .stream()
                .filter(item -> item.getGroup().equals(grp) && item.getAction().equals(act))
                .findFirst()
                .orElse(null);
            
            if(activity!=null) {
	            activitiesTbl.delete(activity.getId());
	
	            activitiesIdx.remove(accId, activity.getId());
            }

            return activity;
        });
    }

    

    /**
     * @param activityKey Activity key.
     * @param activity Activity to save.
     */
    public void save(UUID accId, Activity activity) {
        txMgr.doInTransaction(() -> {

            activitiesTbl.save(activity);

            activitiesIdx.add(accId, activity.getId());
        });
    }
}
