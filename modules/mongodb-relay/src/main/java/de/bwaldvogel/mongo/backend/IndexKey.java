package de.bwaldvogel.mongo.backend;

public class IndexKey {

    private final String key;
    private final boolean ascending;

    public IndexKey(String key, boolean ascending) {
        this.key = key;
        this.ascending = ascending;
    }

    public String getKey() {
        return key;
    }

    public boolean isAscending() {
        return ascending;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[key=" + key + " " + (ascending ? "ASC" : "DESC") + "]";
    }
}
