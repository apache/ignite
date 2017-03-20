package org.apache.ignite.cache.store.hibernate;

import com.google.common.primitives.SignedBytes;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.type.AbstractSingleColumnStandardBasicType;
import org.hibernate.type.VersionType;
import org.hibernate.type.descriptor.java.PrimitiveByteArrayTypeDescriptor;
import org.hibernate.type.descriptor.sql.VarbinaryTypeDescriptor;

import java.util.Comparator;
/**
 * A custom type that maps between a {@link java.sql.Types#VARBINARY VARBINARY} and {@code byte[]} and provides
 * {@link CustomPrimitiveByteArrayTypeDescriptor} with default {@code byte[]} {@link Comparator}
 *
 * @author Mykola Pereyma
 */
public class ByteArrayType extends AbstractSingleColumnStandardBasicType<byte[]> implements VersionType<byte[]> {
    /** Instance of {@link ByteArrayType} */
    public static final ByteArrayType INSTANCE = new ByteArrayType();

    /**
     * Constructs instance of {@link ByteArrayType}
     */
    public ByteArrayType() {
        super(VarbinaryTypeDescriptor.INSTANCE, CustomPrimitiveByteArrayTypeDescriptor.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override
    public String getName() {
        return "binary";
    }

    /** {@inheritDoc} */
    @Override public String[] getRegistrationKeys() {
        return new String[]{getName(), "byte[]", byte[].class.getName()};
    }

    /** {@inheritDoc} */
    @Override public byte[] seed(SessionImplementor session) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public byte[] next(byte[] current, SessionImplementor session) {
        return current;
    }

    /** {@inheritDoc} */
    @Override public Comparator<byte[]> getComparator() {
        return CustomPrimitiveByteArrayTypeDescriptor.INSTANCE.getComparator();
    }

    private static class CustomPrimitiveByteArrayTypeDescriptor extends PrimitiveByteArrayTypeDescriptor {
        public static final CustomPrimitiveByteArrayTypeDescriptor INSTANCE = new CustomPrimitiveByteArrayTypeDescriptor();

        /** {@inheritDoc} */
        @Override public Comparator<byte[]> getComparator() {
            return SignedBytes.lexicographicalComparator();
        }
    }
}