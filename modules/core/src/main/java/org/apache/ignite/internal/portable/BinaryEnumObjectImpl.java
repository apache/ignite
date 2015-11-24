package org.apache.ignite.internal.portable;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;

import java.io.Externalizable;

/**
 * Created by vozerov on 11/24/2015.
 */
public class BinaryEnumObjectImpl implements BinaryObject, Externalizable {
    /** Context. */
    private PortableContext ctx;

    /** Type ID. */
    private int typeId;

    /** Ordinal. */
    private int ord;

    /**
     * {@link Externalizable} support.
     */
    public BinaryEnumObjectImpl() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param typeId Type ID.
     * @param ord Ordinal.
     */
    public BinaryEnumObjectImpl(PortableContext ctx, int typeId, int ord) {
        assert ctx != null;
        assert typeId != 0;

        this.ctx = ctx;
        this.typeId = typeId;
        this.ord = ord;
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return typeId;
    }

    /** {@inheritDoc} */
    @Override public BinaryType type() throws BinaryObjectException {
        return ctx.metadata(typeId);
    }

    /** {@inheritDoc} */
    @Override public <F> F field(String fieldName) throws BinaryObjectException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(String fieldName) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <T> T deserialize() throws BinaryObjectException {
        Class cls = ctx.descriptorForTypeId(true, typeId, null).describedClass();

        return BinaryEnumCache.get(cls, ord);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject clone() throws CloneNotSupportedException {
        return (BinaryObject)super.clone();
    }

    /** {@inheritDoc} */
    @Override public int enumOrdinal() throws BinaryObjectException {
        return ord;
    }


}
