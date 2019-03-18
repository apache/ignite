/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

/**
 * Represents the head of a BNF rule.
 */
public class RuleHead {
    private final String section;
    private final String topic;
    private Rule rule;

    RuleHead(String section, String topic, Rule rule) {
        this.section = section;
        this.topic = topic;
        this.rule = rule;
    }

    public String getTopic() {
        return topic;
    }

    public Rule getRule() {
        return rule;
    }

    void setRule(Rule rule) {
        this.rule = rule;
    }

    public String getSection() {
        return section;
    }

}
