package de.bwaldvogel.mongo.exception;

public class DollarPrefixedFieldNameException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public DollarPrefixedFieldNameException(String message) {
        super(52, "DollarPrefixedFieldName", message);
    }

}
