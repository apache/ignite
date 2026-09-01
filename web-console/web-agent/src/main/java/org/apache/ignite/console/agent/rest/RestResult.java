

package org.apache.ignite.console.agent.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.ignite.console.json.RawContentDeserializer;
import org.apache.ignite.internal.processors.rest.GridRestResponse;

/**
 * Request result.
 */
public class RestResult {
    /** REST http code. */
    private int status;

    /** The field contains description of error if server could not handle the request. */
    private String error;

    /** The field contains result of command. */
    private String data;

    /** Session token string representation. */
    private String sessionToken;
    
    
    private String securitySubjectId = null;

    /**
     * @param status REST http code.
     * @param error The field contains description of error if server could not handle the request.
     * @param sesTok The field contains session token.
     * @param data The field contains result of command.
     */    
    private RestResult(
        @JsonProperty("successStatus") int status,
        @JsonProperty("error") String error,
        @JsonProperty("sessionToken") String sesTok,
        @JsonProperty("response") @JsonDeserialize(using = RawContentDeserializer.class) String data
    ) {
        this.status = status;
        this.error = error;
        this.data = data;
        this.sessionToken = sesTok;
    }
    
    /**
     * @param status REST http code.
     * @param error The field contains description of error if server could not handle the request.
     * @param sesTok The field contains session token.
     * @param data The field contains result of command.
     */
    @JsonCreator
    private RestResult(
        @JsonProperty("successStatus") int status,
        @JsonProperty("error") String error,
        @JsonProperty("sessionToken") String sesTok,
        @JsonProperty("securitySubjectId") String securitySubjectId,
        @JsonProperty("response") @JsonDeserialize(using = RawContentDeserializer.class) String data
    ) {
        this.status = status;
        this.error = error;
        this.data = data;
        this.sessionToken = sesTok;
        this.securitySubjectId = securitySubjectId;
    }

    /**
     * @param status REST http code.
     * @param error The field contains description of error if server could not handle the request.
     * @return Request result.
     */
    public static RestResult fail(int status, String error) {
        return new RestResult(status, error, null, null);
    }
    
    /**
     * @param status REST http code.
     * @param error The field contains description of error if server could not handle the request.
     * @return Request result.
     */
    public static RestResult authFail(String error) {
        return new RestResult(GridRestResponse.STATUS_AUTH_FAILED, error, null, null);
    }

    /**
     * @param data The field contains result of command.
     * @return Request result.
     */
    public static RestResult success(String data, String sesTok) {
        RestResult res = new RestResult(0, null, sesTok, data);
        return res;
    }

    /**
     * @return REST http code.
     */
    public int getStatus() {
        return status;
    }

    /**
     * @return The field contains description of error if server could not handle the request.
     */
    public String getError() {
        return error;
    }

    /**
     * @return The field contains result of command.
     */
    @JsonRawValue
    public String getData() {
        return data;
    }

    /**
     * @return String representation of session token.
     */
    public String getSessionToken() {
        return sessionToken;
    }

	public String getSecuritySubjectId() {
		return securitySubjectId;
	}    
}
