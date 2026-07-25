

package org.apache.ignite.console.messages;

import java.util.Locale;

/**
 * Web console message source.
 */
public class WebConsoleMessageSource extends WildcardReloadableResourceBundleMessageSource {
    /** Message source. */
    private static final WebConsoleMessageSource msgSrc = new WebConsoleMessageSource("UTF-8", "classpath*:i18/*");

    /** Accessor. */
    private static final WebConsoleMessageSourceAccessor accessor = new WebConsoleMessageSourceAccessor(msgSrc, Locale.US);

    /**
     * @param baseNames Base names.
     */
    public WebConsoleMessageSource(String encoding, String... baseNames) {
        setDefaultEncoding(encoding);
        setBasenames(baseNames);
    }

    /**
     * Return message source accessor for web console messages.
     *
     * @return Message Source Accessor.
     */
    public static WebConsoleMessageSourceAccessor getAccessor() {
        return accessor;
    }
}
