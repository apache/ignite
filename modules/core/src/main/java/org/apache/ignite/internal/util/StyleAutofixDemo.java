package org.apache.ignite.internal.util;

import java.util.Map;
import java.util.List;
import org.apache.ignite.IgniteException;
import java.util.ArrayList;
import java.util.HashMap;

/** Demo class for the Ignite PR Checker checkstyle-autofix test — to be removed. */
public class StyleAutofixDemo {
    /** Sample constant. */
    static public final int LIMIT = 10;

    /** Sample state. */
    private final Map<String, List<Integer>> state = new HashMap<>();

    /**
     * Fills the state.
     *
     * @param key Key.
     * @param a First value.
     * @param b Second value.
     */
    public void fill(String key,int a,int b) {
        List<Integer> vals = new ArrayList<>();
		vals.add(a);
        vals.add(b);


        state.put(key, vals);
    }
}