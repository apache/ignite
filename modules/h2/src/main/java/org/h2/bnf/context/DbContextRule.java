/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf.context;

import java.util.HashMap;
import java.util.HashSet;

import org.h2.bnf.Bnf;
import org.h2.bnf.BnfVisitor;
import org.h2.bnf.Rule;
import org.h2.bnf.RuleElement;
import org.h2.bnf.RuleHead;
import org.h2.bnf.RuleList;
import org.h2.bnf.Sentence;
import org.h2.message.DbException;
import org.h2.util.ParserUtil;
import org.h2.util.StringUtils;

/**
 * A BNF terminal rule that is linked to the database context information.
 * This class is used by the H2 Console, to support auto-complete.
 */
public class DbContextRule implements Rule {

    public static final int COLUMN = 0, TABLE = 1, TABLE_ALIAS = 2;
    public static final int NEW_TABLE_ALIAS = 3;
    public static final int COLUMN_ALIAS = 4, SCHEMA = 5, PROCEDURE = 6;

    private final DbContents contents;
    private final int type;

    private String columnType;

    /**
     * BNF terminal rule Constructor
     * @param contents Extract rule from this component
     * @param type Rule type, one of
     * {@link DbContextRule#COLUMN},
     * {@link DbContextRule#TABLE},
     * {@link DbContextRule#TABLE_ALIAS},
     * {@link DbContextRule#NEW_TABLE_ALIAS},
     * {@link DbContextRule#COLUMN_ALIAS},
     * {@link DbContextRule#SCHEMA}
     */
    public DbContextRule(DbContents contents, int type) {
        this.contents = contents;
        this.type = type;
    }

    /**
     * @param columnType COLUMN Auto completion can be filtered by column type
     */
    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    @Override
    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        // nothing to do
    }

    @Override
    public void accept(BnfVisitor visitor) {
        // nothing to do
    }

    @Override
    public boolean autoComplete(Sentence sentence) {
        String query = sentence.getQuery(), s = query;
        String up = sentence.getQueryUpper();
        switch (type) {
        case SCHEMA: {
            DbSchema[] schemas = contents.getSchemas();
            String best = null;
            DbSchema bestSchema = null;
            for (DbSchema schema: schemas) {
                String name = StringUtils.toUpperEnglish(schema.name);
                if (up.startsWith(name)) {
                    if (best == null || name.length() > best.length()) {
                        best = name;
                        bestSchema = schema;
                    }
                } else if (s.length() == 0 || name.startsWith(up)) {
                    if (s.length() < name.length()) {
                        sentence.add(name, name.substring(s.length()), type);
                        sentence.add(schema.quotedName + ".",
                                schema.quotedName.substring(s.length()) + ".",
                                Sentence.CONTEXT);
                    }
                }
            }
            if (best != null) {
                sentence.setLastMatchedSchema(bestSchema);
                s = s.substring(best.length());
            }
            break;
        }
        case TABLE: {
            DbSchema schema = sentence.getLastMatchedSchema();
            if (schema == null) {
                schema = contents.getDefaultSchema();
            }
            DbTableOrView[] tables = schema.getTables();
            String best = null;
            DbTableOrView bestTable = null;
            for (DbTableOrView table : tables) {
                String compare = up;
                String name = StringUtils.toUpperEnglish(table.getName());
                if (table.getQuotedName().length() > name.length()) {
                    name = table.getQuotedName();
                    compare = query;
                }
                if (compare.startsWith(name)) {
                    if (best == null || name.length() > best.length()) {
                        best = name;
                        bestTable = table;
                    }
                } else if (s.length() == 0 || name.startsWith(compare)) {
                    if (s.length() < name.length()) {
                        sentence.add(table.getQuotedName(),
                                table.getQuotedName().substring(s.length()),
                                Sentence.CONTEXT);
                    }
                }
            }
            if (best != null) {
                sentence.setLastMatchedTable(bestTable);
                sentence.addTable(bestTable);
                s = s.substring(best.length());
            }
            break;
        }
        case NEW_TABLE_ALIAS:
            s = autoCompleteTableAlias(sentence, true);
            break;
        case TABLE_ALIAS:
            s = autoCompleteTableAlias(sentence, false);
            break;
        case COLUMN_ALIAS: {
            int i = 0;
            if (query.indexOf(' ') < 0) {
                break;
            }
            for (; i < up.length(); i++) {
                char ch = up.charAt(i);
                if (ch != '_' && !Character.isLetterOrDigit(ch)) {
                    break;
                }
            }
            if (i == 0) {
                break;
            }
            String alias = up.substring(0, i);
            if (ParserUtil.isKeyword(alias)) {
                break;
            }
            s = s.substring(alias.length());
            break;
        }
        case COLUMN: {
            HashSet<DbTableOrView> set = sentence.getTables();
            String best = null;
            DbTableOrView last = sentence.getLastMatchedTable();
            if (last != null && last.getColumns() != null) {
                for (DbColumn column : last.getColumns()) {
                    String compare = up;
                    String name = StringUtils.toUpperEnglish(column.getName());
                    if (column.getQuotedName().length() > name.length()) {
                        name = column.getQuotedName();
                        compare = query;
                    }
                    if (compare.startsWith(name) &&
                            (columnType == null ||
                            column.getDataType().contains(columnType))) {
                        String b = s.substring(name.length());
                        if (best == null || b.length() < best.length()) {
                            best = b;
                        } else if (s.length() == 0 || name.startsWith(compare)) {
                            if (s.length() < name.length()) {
                                sentence.add(column.getName(),
                                        column.getName().substring(s.length()),
                                        Sentence.CONTEXT);
                            }
                        }
                    }
                }
            }
            for (DbSchema schema : contents.getSchemas()) {
                for (DbTableOrView table : schema.getTables()) {
                    if (table != last && set != null && !set.contains(table)) {
                        continue;
                    }
                    if (table == null || table.getColumns() == null) {
                        continue;
                    }
                    for (DbColumn column : table.getColumns()) {
                        String name = StringUtils.toUpperEnglish(column
                                .getName());
                        if (columnType == null
                                || column.getDataType().contains(columnType)) {
                            if (up.startsWith(name)) {
                                String b = s.substring(name.length());
                                if (best == null || b.length() < best.length()) {
                                    best = b;
                                }
                            } else if (s.length() == 0 || name.startsWith(up)) {
                                if (s.length() < name.length()) {
                                    sentence.add(column.getName(),
                                            column.getName().substring(s.length()),
                                            Sentence.CONTEXT);
                                }
                            }
                        }
                    }
                }
            }
            if (best != null) {
                s = best;
            }
            break;
        }
        case PROCEDURE:
            autoCompleteProcedure(sentence);
            break;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        if (!s.equals(query)) {
            while (Bnf.startWithSpace(s)) {
                s = s.substring(1);
            }
            sentence.setQuery(s);
            return true;
        }
        return false;
    }
    private void autoCompleteProcedure(Sentence sentence) {
        DbSchema schema = sentence.getLastMatchedSchema();
        if (schema == null) {
            schema = contents.getDefaultSchema();
        }
        String incompleteSentence = sentence.getQueryUpper();
        String incompleteFunctionName = incompleteSentence;
        if (incompleteSentence.contains("(")) {
            incompleteFunctionName = incompleteSentence.substring(0,
                    incompleteSentence.indexOf('(')).trim();
        }

        // Common elements
        RuleElement openBracket = new RuleElement("(", "Function");
        RuleElement closeBracket = new RuleElement(")", "Function");
        RuleElement comma = new RuleElement(",", "Function");

        // Fetch all elements
        for (DbProcedure procedure : schema.getProcedures()) {
            final String procName = procedure.getName();
            if (procName.startsWith(incompleteFunctionName)) {
                // That's it, build a RuleList from this function
                RuleElement procedureElement = new RuleElement(procName,
                        "Function");
                RuleList rl = new RuleList(procedureElement, openBracket, false);
                // Go further only if the user use open bracket
                if (incompleteSentence.contains("(")) {
                    for (DbColumn parameter : procedure.getParameters()) {
                        if (parameter.getPosition() > 1) {
                            rl = new RuleList(rl, comma, false);
                        }
                        DbContextRule columnRule = new DbContextRule(contents,
                                COLUMN);
                        String parameterType = parameter.getDataType();
                        // Remove precision
                        if (parameterType.contains("(")) {
                            parameterType = parameterType.substring(0,
                                    parameterType.indexOf('('));
                        }
                        columnRule.setColumnType(parameterType);
                        rl = new RuleList(rl, columnRule, false);
                    }
                    rl = new RuleList(rl, closeBracket , false);
                }
                rl.autoComplete(sentence);
            }
        }
    }

    private static String autoCompleteTableAlias(Sentence sentence,
            boolean newAlias) {
        String s = sentence.getQuery();
        String up = sentence.getQueryUpper();
        int i = 0;
        for (; i < up.length(); i++) {
            char ch = up.charAt(i);
            if (ch != '_' && !Character.isLetterOrDigit(ch)) {
                break;
            }
        }
        if (i == 0) {
            return s;
        }
        String alias = up.substring(0, i);
        if ("SET".equals(alias) || ParserUtil.isKeyword(alias)) {
            return s;
        }
        if (newAlias) {
            sentence.addAlias(alias, sentence.getLastTable());
        }
        HashMap<String, DbTableOrView> map = sentence.getAliases();
        if ((map != null && map.containsKey(alias)) ||
                (sentence.getLastTable() == null)) {
            if (newAlias && s.length() == alias.length()) {
                return s;
            }
            s = s.substring(alias.length());
            if (s.length() == 0) {
                sentence.add(alias + ".", ".", Sentence.CONTEXT);
            }
            return s;
        }
        HashSet<DbTableOrView> tables = sentence.getTables();
        if (tables != null) {
            String best = null;
            for (DbTableOrView table : tables) {
                String tableName =
                        StringUtils.toUpperEnglish(table.getName());
                if (alias.startsWith(tableName) &&
                        (best == null || tableName.length() > best.length())) {
                    sentence.setLastMatchedTable(table);
                    best = tableName;
                } else if (s.length() == 0 || tableName.startsWith(alias)) {
                    sentence.add(tableName + ".",
                            tableName.substring(s.length()) + ".",
                            Sentence.CONTEXT);
                }
            }
            if (best != null) {
                s = s.substring(best.length());
                if (s.length() == 0) {
                    sentence.add(alias + ".", ".", Sentence.CONTEXT);
                }
                return s;
            }
        }
        return s;
    }

}
