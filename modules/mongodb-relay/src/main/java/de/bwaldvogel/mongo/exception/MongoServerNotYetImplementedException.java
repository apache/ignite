package de.bwaldvogel.mongo.exception;

public class MongoServerNotYetImplementedException extends MongoServerException {

    private static final long serialVersionUID = 1L;

    private static final String ISSUES_URL = "https://github.com/bwaldvogel/mongo-java-server/issues/";

    public MongoServerNotYetImplementedException(int gitHubIssueNumber, String prefix) {
        super(prefix + " is not yet implemented. See " + ISSUES_URL + gitHubIssueNumber);
    }

}
