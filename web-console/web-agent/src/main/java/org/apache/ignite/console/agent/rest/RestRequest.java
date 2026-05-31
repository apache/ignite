package org.apache.ignite.console.agent.rest;

import java.util.Map;

import io.vertx.core.json.JsonObject;

/**
 * REST request.
 */
public class RestRequest {
    /** Is request for demo cluster. */
    private String clusterId;

    /** REST params. */
    private Map<String,Object> params;

    /**
     * @return value of clusterId
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * @param clusterId Cluster id.
     */
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * @return REST params.
     */
    public Map<String,Object> getParams() {
        return params;
    }

    /**
     * @param params New REST params.
     */
    public void setParams(Map<String,Object> params) {
        this.params = params;
    }
}
