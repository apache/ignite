package de.bwaldvogel.mongo.bson;

import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.bwaldvogel.mongo.backend.Assert;

public class BsonRegularExpression implements Bson {

    private static final long serialVersionUID = 1L;

    private static final String REGEX = "$regex";
    private static final String OPTIONS = "$options";

    private final String pattern;
    private final String options;

    private transient final AtomicReference<Pattern> compiledPattern = new AtomicReference<>(null);

    public BsonRegularExpression(String pattern, String options) {
        this.pattern = pattern;
        this.options = options;
    }

    public BsonRegularExpression(String pattern) {
        this(pattern, null);
    }

    public Document toDocument() {
        Document document = new Document(REGEX, pattern);
        if (options != null) {
            document.append(OPTIONS, options);
        }
        return document;
    }

    private static BsonRegularExpression fromDocument(Document queryObject) {
        String options = "";
        if (queryObject.containsKey(OPTIONS)) {
            options = queryObject.get(OPTIONS).toString();
        }

        String pattern = queryObject.get(REGEX).toString();
        return new BsonRegularExpression(pattern, options);
    }

    public static boolean isRegularExpression(Object object) {
        if (object instanceof Document) {
            return ((Document) object).containsKey(REGEX);
        } else {
            return object instanceof BsonRegularExpression;
        }
    }

    public static BsonRegularExpression convertToRegularExpression(Object pattern) {
        Assert.isTrue(isRegularExpression(pattern), () -> "'" + pattern + "' is not a regular expression");
        if (pattern instanceof BsonRegularExpression) {
            return (BsonRegularExpression) pattern;
        } else {
            return fromDocument((Document) pattern);
        }
    }

    public String getPattern() {
        return pattern;
    }

    public String getOptions() {
        return options;
    }

    private Pattern toPattern() {
        Pattern pattern = compiledPattern.get();
        if (pattern == null) {
            pattern = createPattern();
            compiledPattern.lazySet(pattern);
        }
        return pattern;
    }

    Pattern createPattern() {
        int flags = 0;
        for (char flag : options.toCharArray()) {
            switch (flag) {
                case 'i':
                    flags |= Pattern.CASE_INSENSITIVE;
                    break;
                case 'm':
                    flags |= Pattern.MULTILINE;
                    break;
                case 'x':
                    flags |= Pattern.COMMENTS;
                    break;
                case 's':
                    flags |= Pattern.DOTALL;
                    break;
                case 'u':
                    flags |= Pattern.UNICODE_CASE;
                    break;
                default:
                    throw new IllegalArgumentException("unknown pattern flag: '" + flag + "'");
            }

        }

        // always enable unicode aware case matching
        flags |= Pattern.UNICODE_CASE;

        return Pattern.compile(pattern, flags);
    }

    @Override
    public String toString() {
        Document representative = new Document("$regex", pattern);
        if (options != null) {
            representative.put("$options", options);
        }
        return Json.toJsonValue(representative);
    }

    public Matcher matcher(String string) {
        return toPattern().matcher(string);
    }
}
