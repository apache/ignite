/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.Locale;

import org.h2.message.DbException;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

/**
 * An implementation of CompareMode that uses the ICU4J Collator.
 */
public class CompareModeIcu4J extends CompareMode {

    private final Comparator<String> collator;

    protected CompareModeIcu4J(String name, int strength, boolean binaryUnsigned) {
        super(name, strength, binaryUnsigned);
        collator = getIcu4jCollator(name, strength);
    }

    @Override
    public int compareString(String a, String b, boolean ignoreCase) {
        if (ignoreCase) {
            a = a.toUpperCase();
            b = b.toUpperCase();
        }
        return collator.compare(a, b);
    }

    @Override
    public boolean equalsChars(String a, int ai, String b, int bi,
            boolean ignoreCase) {
        return compareString(a.substring(ai, ai + 1), b.substring(bi, bi + 1),
                ignoreCase) == 0;
    }

    @SuppressWarnings("unchecked")
    private static Comparator<String> getIcu4jCollator(String name, int strength) {
        try {
            Comparator<String> result = null;
            Class<?> collatorClass = JdbcUtils.loadUserClass(
                    "com.ibm.icu.text.Collator");
            Method getInstanceMethod = collatorClass.getMethod(
                    "getInstance", Locale.class);
            if (name.length() == 2) {
                Locale locale = new Locale(StringUtils.toLowerEnglish(name), "");
                if (compareLocaleNames(locale, name)) {
                    result = (Comparator<String>) getInstanceMethod.invoke(null, locale);
                }
            } else if (name.length() == 5) {
                // LL_CC (language_country)
                int idx = name.indexOf('_');
                if (idx >= 0) {
                    String language = StringUtils.toLowerEnglish(name.substring(0, idx));
                    String country = name.substring(idx + 1);
                    Locale locale = new Locale(language, country);
                    if (compareLocaleNames(locale, name)) {
                        result = (Comparator<String>) getInstanceMethod.invoke(null, locale);
                    }
                }
            }
            if (result == null) {
                for (Locale locale : (Locale[]) collatorClass.getMethod(
                        "getAvailableLocales").invoke(null)) {
                    if (compareLocaleNames(locale, name)) {
                        result = (Comparator<String>) getInstanceMethod.invoke(null, locale);
                        break;
                    }
                }
            }
            if (result == null) {
                throw DbException.getInvalidValueException("collator", name);
            }
            collatorClass.getMethod("setStrength", int.class).invoke(result, strength);
            return result;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

}
