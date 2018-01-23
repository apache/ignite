package org.apache.ignite.ml.idd;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class IDDInstanceLocalStorage {

    private static final IDDInstanceLocalStorage instance = new IDDInstanceLocalStorage();

    private final ConcurrentMap<UUID, ConcurrentMap<Integer, Object>> storage = new ConcurrentHashMap<>();

    public static IDDInstanceLocalStorage getInstance() {
        return instance;
    }

    public ConcurrentMap<Integer, Object> getOrCreateIDDLocalStorage(UUID iddId) {
        return storage.computeIfAbsent(iddId, i -> new ConcurrentHashMap<>());
    }
}
