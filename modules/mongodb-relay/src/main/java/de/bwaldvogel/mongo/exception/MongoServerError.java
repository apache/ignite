package de.bwaldvogel.mongo.exception;

public class MongoServerError extends MongoServerException {

    private static final long serialVersionUID = 1L;

    private final String message;
    private final int errorCode;
    private final String codeName;

    public MongoServerError(int errorCode, String message) {
        this(errorCode, "Location" + errorCode, message);
    }

    public MongoServerError(int errorCode, String codeName, String message) {
        super("[Error " + errorCode + "] " + message);
        this.errorCode = errorCode;
        this.codeName = codeName;
        this.message = message;
    }

    public int getCode() {
        return errorCode;
    }

    public String getCodeName() {
        return codeName;
    }

    @Override
    public String getMessageWithoutErrorCode() {
        return message;
    }
}
