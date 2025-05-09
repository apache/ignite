package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.core.json.JsonObject;

public class ServiceResult {
	private String status;
	private String error;	
	List<String> messages = new ArrayList<>();
	JsonObject result = new JsonObject();

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

	public JsonObject getResult() {
		return result;
	}
	
	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}
	
	public JsonObject toJson() {
		JsonObject stat = new JsonObject();
		stat.put("status", status);
		if(error!=null) {
			stat.put("error", error);
		}
		stat.put("result", result);
		if(messages.isEmpty()) {
			stat.put("message", null);
		}
		else {
			stat.put("message", String.join("\n",messages));
		}
		return stat;
	}
}
