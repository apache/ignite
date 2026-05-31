package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.vertx.core.json.JsonObject;


/**
 1. 通用写操作返回参数（WriteResult 或 BulkWriteResult）

insertedCount:

插入操作中而插入的新文档数量。

insertedId / insertedIds:

插入操作返回新文档的 _id（单条插入返回 insertedId，批量插入返回 insertedIds 数组）。

matchedCount:

更新操作中匹配到的文档数量。

modifiedCount:

更新操作中实际修改的文档数量（可能小于 matchedCount，例如文档已符合更新条件但内容未变化）。

upsertedCount:

更新操作中因启用 upsert: true 而插入的新文档数量。

upsertedId:

更新操作中因 upsert: true 而插入的新文档的 _id。

deletedCount:

删除操作中删除的文档数量。

3. 批量操作（BulkWrite）额外参数
nInserted: 插入的文档数量。

nUpdated: 更新的文档数量。

nRemoved: 删除的文档数量。

nUpserted: 通过 upsert 插入的文档数量。
 * 
 *
 */
public class ServiceResult {
	// acknowledged: 布尔值，表示操作是否被 Service 确认（取决于是否启用了写关注 writeConcern）。
	private boolean acknowledged = true;
	private String status;
	private String errorType;	
	private List<String> messages = null;
	private Map<String,Object> result = null;
	private String cacheName;
	
	public static ServiceResult fail(String errorType) {
		ServiceResult result = new ServiceResult();
		result.setErrorType(errorType);
		result.setAcknowledged(false);
		return result;
	}
	
	public static ServiceResult success(String status) {
		ServiceResult result = new ServiceResult();
		result.setStatus(status);
		return result;
	}

	public ServiceResult setStatus(String status) {
		this.status = status;
		return this;
	}

	public ServiceResult put(String key,Object value) {
		getResult().put(key, value);
		return this;
	}
	
	public ServiceResult addMessage(String msg) {
		getMessages().add(msg);
		return this;
	}
	

	public String getStatus() {
		return status;
	}
	
	public List<String> getMessages() {
		if(messages==null) {
			messages = new ArrayList<>();
		}
		return messages;
	}

	public Map<String,Object> getResult() {
		if(result==null) {
			result = new LinkedHashMap<String,Object>();
		}
		return result;
	}
	
	public String getErrorType() {
		return errorType;
	}

	public void setErrorType(String errorType) {
		this.errorType = errorType;
	}
	
	public JsonObject toJson() {
		JsonObject stat = new JsonObject();
		if(this.acknowledged && status==null) {
			stat.put("status", "sucess");
		}
		else {
			stat.put("status", status);
		}
		if(errorType!=null) {
			stat.put("errorType", errorType);
		}
		stat.put("result", result);
		if(messages==null || messages.isEmpty()) {
			stat.put("message", null);
		}
		else {
			stat.put("message", String.join("\n",messages));
		}
		return stat;
	}

	public boolean isAcknowledged() {
		return acknowledged;
	}

	public void setAcknowledged(boolean acknowledged) {
		this.acknowledged = acknowledged;
	}

	public String getCacheName() {
		return cacheName;
	}

	public void setCacheName(String cacheName) {
		this.cacheName = cacheName;
	}
}
