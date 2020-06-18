package de.bwaldvogel.mongo.exception;

import de.bwaldvogel.mongo.backend.Assert;

public class MongoServerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private boolean logError = true;

    public MongoServerException(String message) {
        super(validateMessage(message));
    }

    public MongoServerException(String message, Throwable cause) {
        super(validateMessage(message), cause);
    }

    private static String validateMessage(String message) {
        Assert.notNullOrEmpty(message, () -> "Illegal error message");
        return message;
    }

    public String getMessageWithoutErrorCode() {
        return getMessage();
    }

    public void setLogError(boolean logError) {
        this.logError = logError;
    }

    public boolean isLogError() {
        return logError;
    }

}
