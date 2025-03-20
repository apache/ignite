package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.core.json.JsonObject;

public class ServiceResult {
	String status;
	List<String> messages = new ArrayList<>();
	Map<String, Object> result = new HashMap<>();

	public ServiceResult setStatus(String status) {
		this.status = status;
		return this;
	}

	public ServiceResult put(String key,Object value) {
		result.put(key, value);
		return this;
	}
	
	public ServiceResult addMessage(String msg) {
		messages.add(msg);
		return this;
	}
	

	public String getStatus() {
		return status;
	}
	
	public List<String> getMessages() {
		return messages;
	}

	public Map<String, Object> getResult() {
		return result;
	}
	
	public JsonObject toJson() {
		JsonObject stat = new JsonObject();
		stat.put("status", status);
		stat.put("result", result);
		stat.put("message", messages);
		return stat;
	}
}
