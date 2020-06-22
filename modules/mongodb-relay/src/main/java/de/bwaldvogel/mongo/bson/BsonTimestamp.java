package de.bwaldvogel.mongo.bson;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.Instant;

public class BsonTimestamp implements Bson,Externalizable,Comparable<BsonTimestamp> {

    private static final long serialVersionUID = 1L;

    private long value;

    protected BsonTimestamp() {
    }

    public BsonTimestamp(long timestamp) {
        this.value = timestamp;
    }


    public BsonTimestamp(Instant instant, int increment) {
        value = (instant.getEpochSecond() << 32) | (increment & 0xFFFFFFFFL);
    }

    public int getTime() {
        return (int) (value >> 32);
    }
	
	public long getValue() {
        return value;
    }


    public long getTimestamp() {
        return value;
    }
	
    public int getInc() {
        return (int) value;
    }
    @Override
    public String toString() {
        return "BsonTimestamp[value=" + getValue()
            + ", seconds=" + getTime()
            + ", inc=" + getInc()
            + "]";
    }

    @Override
    public int compareTo(BsonTimestamp other) {
        return Long.compareUnsigned(value, other.value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BsonTimestamp other = (BsonTimestamp) o;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }
	

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(value);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		value = in.readLong();
	}

}
