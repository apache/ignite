package org.apache.ignite.testframework.junits.common;

/**
 * Defined type of key
 * */
public enum KeyType {
    PRIMARY(0),
    BACKUP(1),
    NEAR(2);

    private int keyIndex;

    KeyType(int keyIndex) {
        this.keyIndex = keyIndex;
    }

    public int getKeyIndex() {
        return keyIndex;
    }
}
