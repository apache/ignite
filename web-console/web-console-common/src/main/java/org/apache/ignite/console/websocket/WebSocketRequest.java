

package org.apache.ignite.console.websocket;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.ignite.console.json.RawContentDeserializer;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.console.utils.Utils.extractErrorMessage;
import static org.apache.ignite.console.websocket.WebSocketEvents.ERROR;

/**
 * Websocket request POJO.
 */
public class WebSocketRequest implements WebSocketEvent<String> {
    /** */
    private String reqId;

    /** */
    private String evtType;

    /** */
    @GridToStringInclude
    private String payload;
    
    /** account token */
    private String token;

    /** {@inheritDoc} */
    @Override public String getRequestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public void setRequestId(String reqId) {
        this.reqId = reqId;
    }

    /** {@inheritDoc} */
    @Override public String getEventType() {
        return evtType;
    }

    /** {@inheritDoc} */
    @Override public void setEventType(String evtType) {
        this.evtType = evtType;
    }

    /** {@inheritDoc} */
    @JsonRawValue
    @Override public String getPayload() {
        return payload;
    }

    /** {@inheritDoc} */
    @JsonDeserialize(using = RawContentDeserializer.class)
    @Override public void setPayload(String payload) {
        this.payload = payload;
    }

    /**
     * Create event with payload for response with same ID.
     *
     * @param payload Payload.
     */
    public WebSocketResponse response(Object payload) {
        return new WebSocketResponse(reqId, evtType, payload);
    }

    /**
     * Create event with error for response with same ID.
     *
     * @param prefix Message prefix.
     * @param e Exception.
     */
    public WebSocketResponse withError(String prefix, Throwable e) {
        Map<String, String> err = new HashMap<>();

        err.put("message", e != null ? extractErrorMessage(prefix, e) : prefix);

        return new WebSocketResponse(getRequestId(), ERROR, err);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebSocketRequest.class, this);
    }

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}
}
