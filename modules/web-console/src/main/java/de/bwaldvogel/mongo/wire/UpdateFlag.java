package de.bwaldvogel.mongo.wire;

public enum UpdateFlag {
    UPSERT(0), MULTI_UPDATE(1);

    private int value;

    UpdateFlag(int bit) {
        this.value = 1 << bit;
    }

    public boolean isSet(int flags) {
        return (flags & value) == value;
    }

}
