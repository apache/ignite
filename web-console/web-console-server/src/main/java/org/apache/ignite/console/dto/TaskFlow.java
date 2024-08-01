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

package org.apache.ignite.console.dto;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Task flow define info.
 */
public class TaskFlow extends AbstractDto {    

	/** */
    private UUID accId;

    /** 任务组，需要同时启动和管理的任务，task group 和 to clusterId 等价 */
    private String grp;

    /** */
    private String act; //action
    
    /** from cluster */
    private String sourceCluster;
    
    /** from cache */
    private String source;
    
    /** to cluster */
    private String targetCluster;
    
    /** to cache */
    private String target;
    
    /** targetField -> fromField */
    private Map<String,String> fieldsMap;
    
    /** cron expr */
    private String cronString;
    
    /** EXISTING更新策略: 
     * SKIP_EXISTING,
     * REPLACE_EXISTING,
     * UPDATE_EXISTING,
     * MERGE_EXISTING 
     **/
    private String existingMode;
    /**
     * 传输时，cache级别或者item级别原子操作
     */
    private String atomicityMode;
    
    /**
     * 是否从备份库读取
     */
    private boolean readFromBackup;

	/** 采集数据量 */
    private int amount;
    
    /** 任务创建时间 */
    private long created;
    
    /** 采集更新时间 */
    private long updated;

    /**
     * Default constructor for serialization.
     */
    public TaskFlow() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param id Activity ID.
     * @param accId Account ID.
     * @param grp Group.
     * @param act Activity.
     * @param amount Number of times activity was executed in current period..
     */
    public TaskFlow(UUID id, UUID accId, String grp, String act, int amount) {
        super(id);

        this.accId = accId;
        this.grp = grp;
        this.act = act;
        this.amount = amount;
    }

    /**
     * @return Account ID.
     */
    public UUID getAccountId() {
        return accId;
    }

    /**
     * @param accId Account ID.
     */
    public void setAccountId(UUID accId) {
        this.accId = accId;
    }

    /**
     * @return Activity group.
     */
    public String getGroup() {
        return grp;
    }

    /**
     * @param grp Activity group.
     */
    public void setGroup(String grp) {
        this.grp = grp;
    }

    /**
     * @return Activity action.
     */
    public String getAction() {
        return act;
    }

    /**
     * @param act Activity action.
     */
    public void setAction(String act) {
        this.act = act;
    }

    /**
     * @return Number of times activity was executed in current period.
     */
    public int getAmount() {
        return amount;
    }

    /**
     * @param amount Number of times activity was executed in current period.
     */
    public void setAmount(int amount) {
        this.amount = amount;
    }

    /**
     * Increment number of activity usages in current period.
     *
     * @param inc Value to add to amount.
     */
    public void increment(int inc) {
        amount += inc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TaskFlow.class, this);
    }
    


	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public long getCreated() {
		return created;
	}

	public void setCreated(long created) {
		this.created = created;
	}

	public long getUpdated() {
		return updated;
	}

	public void setUpdated(long updated) {
		this.updated = updated;
	}

	public String getSourceCluster() {
		return sourceCluster;
	}

	public void setSourceCluster(String sourceCluster) {
		this.sourceCluster = sourceCluster;
	}
	
	public String getTargetCluster() {
		return targetCluster;
	}

	public void setTargetCluster(String targetCluster) {
		this.targetCluster = targetCluster;
	}

	public Map<String, String> getFieldsMap() {
		return fieldsMap;
	}

	public void setFieldsMap(Map<String, String> fieldsMap) {
		this.fieldsMap = fieldsMap;
	}

	public String getCronString() {
		return cronString;
	}

	public void setCronString(String cronString) {
		this.cronString = cronString;
	}

	public String getExistingMode() {
		return existingMode;
	}

	public void setExistingMode(String existingMode) {
		this.existingMode = existingMode;
	}

	public String getAtomicityMode() {
		return atomicityMode;
	}

	public void setAtomicityMode(String atomicityMode) {
		this.atomicityMode = atomicityMode;
	}

	public boolean isReadFromBackup() {
		return readFromBackup;
	}

	public void setReadFromBackup(boolean readFromBackup) {
		this.readFromBackup = readFromBackup;
	}	
}
