package de.bwaldvogel.mongo.bson;

import static de.bwaldvogel.mongo.wire.BsonConstants.LENGTH_OBJECTID;

import java.security.SecureRandom;
import java.util.Arrays;

import de.bwaldvogel.mongo.backend.Assert;

public class ObjectId implements Bson, Comparable<ObjectId> {

    private static final long serialVersionUID = 1L;

    private final byte[] data = new byte[LENGTH_OBJECTID];

    private static final SecureRandom random = new SecureRandom();

    public ObjectId() {
        random.nextBytes(data);
    }

    public ObjectId(byte[] data) {
        Assert.equals(data.length, LENGTH_OBJECTID, () -> "Length must be " + LENGTH_OBJECTID + " but was " + data.length);
        System.arraycopy(data, 0, this.data, 0, this.data.length);
    }

    public byte[] toByteArray() {
        return data;
    }

    @Override
    public int compareTo(final ObjectId other) {
        byte[] byteArray = toByteArray();
        byte[] otherByteArray = other.toByteArray();
        for (int i = 0; i < LENGTH_OBJECTID; i++) {
            if (byteArray[i] != otherByteArray[i]) {
                int thisByte = byteArray[i] & 0xFF;
                int otherByte = otherByteArray[i] & 0xFF;
                return (thisByte < otherByte) ? -1 : 1;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ObjectId objectId = (ObjectId) o;

        return Arrays.equals(data, objectId.data);

    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getHexData() + "]";
    }

    public String getHexData() {
        StringBuilder sb = new StringBuilder();
        for (byte b : data) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
