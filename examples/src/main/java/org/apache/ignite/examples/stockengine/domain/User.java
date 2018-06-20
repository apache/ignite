package org.apache.ignite.examples.stockengine.domain;

public class User {
    private final int id;
    private final String name;
    private final String language;

    public User(int id, String name, String language) {
        this.id = id;
        this.name = name;
        this.language = language;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getLanguage() {
        return language;
    }
}
