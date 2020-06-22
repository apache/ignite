package de.bwaldvogel.mongo.wire.message;

public class MessageHeader {

    private final int totalLength;
    private final int requestID;
    private final int responseTo;

    public MessageHeader(int requestID, int responseTo) {
        this(0, requestID, responseTo);
    }

    public MessageHeader(int totalLength, int requestID, int responseTo) {
        this.totalLength = totalLength;
        this.requestID = requestID;
        this.responseTo = responseTo;
    }

    public int getTotalLength() {
        return totalLength;
    }

    public int getRequestID() {
        return requestID;
    }

    public int getResponseTo() {
        return responseTo;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("(");
        sb.append("request: ").append(requestID);
        sb.append(", responseTo: ").append(responseTo);
        if (totalLength > 0) {
            sb.append(", length: ").append(totalLength);
        }
        sb.append(")");
        return sb.toString();
    }

}
