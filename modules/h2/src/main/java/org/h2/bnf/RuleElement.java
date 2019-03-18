/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;

import org.h2.util.StringUtils;

/**
 * A single terminal rule in a BNF object.
 */
public class RuleElement implements Rule {

    private final boolean keyword;
    private final String name;
    private Rule link;
    private final int type;

    public RuleElement(String name, String topic) {
        this.name = name;
        this.keyword = name.length() == 1 ||
                name.equals(StringUtils.toUpperEnglish(name));
        topic = StringUtils.toLowerEnglish(topic);
        this.type = topic.startsWith("function") ?
                Sentence.FUNCTION : Sentence.KEYWORD;
    }

    @Override
    public void accept(BnfVisitor visitor) {
        visitor.visitRuleElement(keyword, name, link);
    }

    @Override
    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        if (link != null) {
            link.setLinks(ruleMap);
        }
        if (keyword) {
            return;
        }
        String test = Bnf.getRuleMapKey(name);
        for (int i = 0; i < test.length(); i++) {
            String t = test.substring(i);
            RuleHead r = ruleMap.get(t);
            if (r != null) {
                link = r.getRule();
                return;
            }
        }
        throw new AssertionError("Unknown " + name + "/" + test);
    }

    @Override
    public boolean autoComplete(Sentence sentence) {
        sentence.stopIfRequired();
        if (keyword) {
            String query = sentence.getQuery();
            String q = query.trim();
            String up = sentence.getQueryUpper().trim();
            if (up.startsWith(name)) {
                query = query.substring(name.length());
                while (!"_".equals(name) && Bnf.startWithSpace(query)) {
                    query = query.substring(1);
                }
                sentence.setQuery(query);
                return true;
            } else if (q.length() == 0 || name.startsWith(up)) {
                if (q.length() < name.length()) {
                    sentence.add(name, name.substring(q.length()), type);
                }
            }
            return false;
        }
        return link.autoComplete(sentence);
    }

}
