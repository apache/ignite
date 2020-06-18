package de.bwaldvogel.mongo.exception;

public class NoReplicationEnabledException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public NoReplicationEnabledException() {
        super(76, "NoReplicationEnabled", "not running with --replSet");
        setLogError(false);
    }

}
