package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;

/**
 * The exception indicates a duplicate type ID was encountered.
 */
public class DuplicateTypeIdException extends IgniteCheckedException {
    /** Platform ID */
    private final byte platformId;

    /** Type ID */
    private final int typeId;

    /** Name of already registered class */
    private final String oldClsName;

    /** Name of new class being registered */
    private final String newClsName;

    /**
     * Initializes new instance of {@link DuplicateTypeIdException} class.
     *
     * @param platformId Platform ID
     * @param typeId Type ID
     * @param oldClsName Name of already registered class
     * @param newClsName Name of new class being registered
     */
    DuplicateTypeIdException(byte platformId, int typeId, String oldClsName, String newClsName) {
        this.platformId = platformId;
        this.typeId = typeId;
        this.oldClsName = oldClsName;
        this.newClsName = newClsName;
    }

    /**
     * {@inheritDoc}
     */
    @Override public String getMessage() {
        return "Duplicate ID [platformId="
            + platformId
            + ", typeId="
            + typeId
            + ", oldCls="
            + oldClsName
            + ", newCls="
            + newClsName + "]";
    }

    /**
     * @return Name of already registered class
     */
    public String getRegisteredClassName() {
        return oldClsName;
    }
}