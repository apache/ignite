package de.bwaldvogel.mongo.exception;

public class ConflictingUpdateOperatorsException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public ConflictingUpdateOperatorsException(String message) {
        super(40, "ConflictingUpdateOperators", message);
    }

}
