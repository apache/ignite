package org.apache.ignite.testframework;

import org.jetbrains.annotations.NotNull;

import java.util.regex.Pattern;

/** FIXME */
public class StringOrPattern {
    private final String s;
    private final Pattern p;

    /** FIXME */
    private StringOrPattern(@NotNull String s) {
        assert s != null;
        this.s = s;
        this.p = null;
    }

    /** FIXME */
    private StringOrPattern(@NotNull Pattern p) {
        assert p != null;
        this.s = null;
        this.p = p;
    }

    /** FIXME */
    public String s() {
        assert s != null;
        return s;
    }

    /** FIXME */
    public Pattern p() {
        assert p != null;
        return p;
    }

    /** FIXME */
    public boolean isString() {
        return s != null;
    }

    /** FIXME */
    public boolean isPattern() {
        return p != null;
    }

    @Override public String toString() {
        return isString() ? ('"' + s() + '"') : ('/' + p().toString() + '/');
    }

    /** FIXME */
    public static StringOrPattern of(String s) {
        return new StringOrPattern(s);
    }

    /** FIXME */
    public static StringOrPattern of(Pattern p) {
        return new StringOrPattern(p);
    }

    /** FIXME */
    public static StringOrPattern ofNullable(String s) {
        return s == null ? null : of(s);
    }

    /** FIXME */
    public static StringOrPattern ofNullable(Pattern p) {
        return p == null ? null : of(p);
    }

    /** FIXME */
    public static StringOrPattern ofPattern(String re) {
        return of(Pattern.compile(re));
    }
}
