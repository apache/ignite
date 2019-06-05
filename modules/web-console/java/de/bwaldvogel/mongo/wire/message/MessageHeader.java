package de.bwaldvogel.mongo.wire.message;

public class MessageHeader {

    private final int requestID;
    private final int responseTo;

    public MessageHeader(int requestID, int responseTo) {
        this.requestID = requestID;
        this.responseTo = responseTo;
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
        sb.append(")");
        return sb.toString();
    }

}
