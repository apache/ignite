package org.apache.ignite.examples.aep;


import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Entity class for benchmark.
 */
public class SampleValue implements Externalizable, Binarylizable {
    /** */
    @QuerySqlField
    private int id;

    /** */
    public SampleValue() {
        // No-op.
    }

    /**
     * @param id Id.
     */
    public SampleValue(int id) {
        this.id = id;
    }

    /**
     * @param id Id.
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return Id.
     */
    public int getId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        writer.writeInt("id", id);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        id = reader.readInt("id");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "SampleValue [id=" + id + ']';
    }
}