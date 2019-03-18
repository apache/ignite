/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.indexer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Represents a word of the full text index.
 */
public class Word {

    /**
     * The word text.
     */
    String name;

    /**
     * The pages map.
     */
    final HashMap<Page, Weight> pages = new HashMap<>();

    private ArrayList<Weight> weightList;

    Word(String name) {
        this.name = name;
    }

    /**
     * Add a page to this word.
     *
     * @param page the page
     * @param weight the weight of this word in this page
     */
    void addPage(Page page, int weight) {
        Weight w = pages.get(page);
        if (w == null) {
            w = new Weight();
            w.page = page;
            pages.put(page, w);
        }
        w.value += weight;
        page.relations++;
    }

    @Override
    public String toString() {
        return name + ":" + pages;
    }

    /**
     * Add all data of the other word to this word.
     *
     * @param other the other word
     */
    void addAll(Word other) {
        for (Entry<Page, Weight> entry : other.pages.entrySet()) {
            Page p = entry.getKey();
            Weight w = entry.getValue();
            addPage(p, w.value);
        }
    }

    ArrayList<Weight> getSortedWeights() {
        if (weightList == null) {
            weightList = new ArrayList<>(pages.values());
            Collections.sort(weightList, new Comparator<Weight>() {
                @Override
                public int compare(Weight w0, Weight w1) {
                    return Integer.compare(w1.value, w0.value);
                }
            });
        }
        return weightList;
    }
}
