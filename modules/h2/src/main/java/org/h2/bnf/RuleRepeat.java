/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;

/**
 * Represents a loop in a BNF object.
 */
public class RuleRepeat implements Rule {

    private final Rule rule;
    private final boolean comma;

    public RuleRepeat(Rule rule, boolean comma) {
        this.rule = rule;
        this.comma = comma;
    }

    @Override
    public void accept(BnfVisitor visitor) {
        visitor.visitRuleRepeat(comma, rule);
    }

    @Override
    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        // not required, because it's already linked
    }

    @Override
    public boolean autoComplete(Sentence sentence) {
        sentence.stopIfRequired();
        while (rule.autoComplete(sentence)) {
            // nothing to do
        }
        String s = sentence.getQuery();
        while (Bnf.startWithSpace(s)) {
            s = s.substring(1);
        }
        sentence.setQuery(s);
        return true;
    }

}
