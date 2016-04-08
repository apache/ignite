package org.apache.ignite.yardstick.cache.load;

import java.lang.reflect.Constructor;

/**
 * Created by pyatkov-vd on 07.04.2016.
 */
public class CacheFrame {

    private Constructor keyConstructor;
    private Constructor valueConstructor;
    private boolean isTransactional = false;

    public Constructor getKeyConstructor() {
        return keyConstructor;
    }

    public void setKeyConstructor(Constructor keyConstructor) {
        this.keyConstructor = keyConstructor;
    }

    public Constructor getValueConstructor() {
        return valueConstructor;
    }

    public void setValueConstructor(Constructor valueConstructor) {
        this.valueConstructor = valueConstructor;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    public void setTransactional(boolean transactional) {
        isTransactional = transactional;
    }
}
