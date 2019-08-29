package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;

public class LimitedList<E> extends ArrayList<E> {

    private static final long serialVersionUID = -4265811949513159615L;

    private int limit;

    LimitedList(int limit) {
        this.limit = limit;
    }

    @Override
    public boolean add(E o) {
        super.add(o);
        while (size() > limit) {
            super.remove(0);
        }
        return true;
    }
}