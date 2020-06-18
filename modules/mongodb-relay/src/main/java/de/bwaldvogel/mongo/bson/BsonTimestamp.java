package de.bwaldvogel.mongo.bson;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class BsonTimestamp implements Bson,Externalizable {

    private static final long serialVersionUID = 1L;

    private long timestamp;

    protected BsonTimestamp() {
    }

    public BsonTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(timestamp);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		timestamp = in.readLong();
	}

}
