

package org.apache.ignite.console.websocket;

/**
 * Websocket event with payload.
 *
 * @param <T> payload type.
 */
public interface WebSocketEvent<T> {
    /**
     * @return Request ID.
     */
    public String getRequestId();

    /**
     * @param reqId New request ID.
     */
    public void setRequestId(String reqId);

    /**
     * @return Event type.
     */
    public String getEventType();

    /**
     * @param evtType New event type.
     */
    public void setEventType(String evtType);

    /**
     * @return Payload.
     */
    public T getPayload();

    /**
     * @param payload New payload.
     */
    public void setPayload(T payload);
}
