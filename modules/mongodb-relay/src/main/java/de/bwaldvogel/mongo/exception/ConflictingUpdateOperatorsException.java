package de.bwaldvogel.mongo.exception;

public class ConflictingUpdateOperatorsException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public ConflictingUpdateOperatorsException(String updatePath, String conflictingPath) {
        super(40, "ConflictingUpdateOperators",
            "Updating the path '" + updatePath + "' would create a conflict at '" + conflictingPath + "'");
    }

}
