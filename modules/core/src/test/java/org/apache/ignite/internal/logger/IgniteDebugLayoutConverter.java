package org.apache.ignite.internal.logger;

import java.awt.Color;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.pattern.AnsiEscape;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.logging.log4j.core.pattern.PatternFormatter;
import org.apache.logging.log4j.core.pattern.PatternParser;

/**
 * Ignite Test framework layout converter, which can be used to highlight different nodes with different colors
 * for easier log analysis.
 * <p>
 * Set {@code IGNITE_DEBUG_ANSI_CONSOLE_TYPE=light} environment proprety to enable highlighting for the light console.
 * <p>
 * Set {@code IGNITE_DEBUG_ANSI_CONSOLE_TYPE=dark} environment property to enable highlighting for the dark console.
 */
@Plugin(name = "igniteDebugConverter", category = PatternConverter.CATEGORY)
@ConverterKeys({"igniteDebugConverter"})
public class IgniteDebugLayoutConverter extends LogEventPatternConverter {
    /** */
    private static final String IGNITE_DEBUG_ANSI_CONSOLE_TYPE = "IGNITE_DEBUG_ANSI_CONSOLE_TYPE";

    /** Disable ANSI colors in console output system property. */
    private static final String IGNITE_NO_LOG_COLOR = "IGNITE_NO_LOG_COLOR";

    /** Pattern formatters. */
    private final List<PatternFormatter> patternFormatters;

    /** Node index -> background color. */
    private final Map<Integer, String> colorByNodeIdxCache = new HashMap<>();

    /** Test node instance name -> node index. */
    private final Map<String, Integer> nodeIdxByNameCache = new HashMap<>();

    /** Force hdie ANSI flag. */
    private static boolean hideAnsi;

    /** Light console flag. */
    private static Boolean lightConsole;

    /** 10 most different colors */
    private static final float colors[] = {
        /* green */ 0.4f,
        /* cyan */ 0.5f,
        /* blue */ 0.63f,
        /* violet */ 0.78f,
        /* rose */ 0.9f,
        /* red */ 0.0f,
        /* brown */ 0.06f,
        /* orange */ 0.13f,
        /* yellow */ 0.18f,
        /* l-green */ 0.24f
    };

    /**
     * @param patternFormatters Pattern formatters.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    protected IgniteDebugLayoutConverter(final List<PatternFormatter> patternFormatters) {
        super("style", "style");

        this.patternFormatters = patternFormatters;
    }

    /**
     * @param config Logger configuration.
     * @param options Layout options.
     * @return Layout converter instance.
     */
    public static IgniteDebugLayoutConverter newInstance(final Configuration config, final String[] options) {
        if (options.length < 1) {
            LOGGER.error("Incorrect number of options on style. Expected at least 1, received " + options.length);

            return null;
        }

        if (options[0] == null) {
            LOGGER.error("No pattern supplied on style");

            return null;
        }

        boolean disableAnsi = Boolean.parseBoolean(System.getProperty(IGNITE_NO_LOG_COLOR));
        String consTypeStr = System.getProperty(IGNITE_DEBUG_ANSI_CONSOLE_TYPE, "").toUpperCase();

        if ("LIGHT".equals(consTypeStr))
            lightConsole = true;
        else if ("DARK".equals(consTypeStr))
            lightConsole = false;

        hideAnsi = lightConsole == null || disableAnsi;

        PatternParser parser = PatternLayout.createPatternParser(config);

        return new IgniteDebugLayoutConverter(parser.parse(options[0], false, disableAnsi, false));
    }

    /**
     * @return The index of the node that is currently logging ({@code -1} if node index cannot be detected).
     */
    private int nodeIndex() {
        String threadName = Thread.currentThread().getName();

        int lastIdx = threadName.length() - 1;

        if (threadName.charAt(lastIdx) != '%')
            return -1;

        int pos = threadName.lastIndexOf('%', lastIdx - 1);

        if (pos == -1)
            return -1;

        String instanceName = threadName.substring(pos + 1, lastIdx);

        return nodeIdxByNameCache.computeIfAbsent(instanceName, v -> {
            int n = 0;
            int res = 0;

            for (int pos0 = instanceName.length() - 1; pos0 >= 0; pos0--) {
                char cur = instanceName.charAt(pos0);

                if (cur < '0' || cur > '9')
                    return res;

                res += (cur - '0') * Math.pow(10, n++);
            }

            return res;
        });
    }

    /**
     * @param light Light console flag.
     * @param nodeIdx The index of the node that is currently logging.
     * @return Background color name.
     */
    private String backgroundColor(boolean light, int nodeIdx) {
        return nodeIdx == -1 ? null : colorByNodeIdxCache.computeIfAbsent(nodeIdx, v -> {
            int power = nodeIdx / colors.length;
            float hue = colors[nodeIdx % colors.length];
            float sat = light ? 17 + Math.min(20, power * 2) : 100 - Math.min(100, power * 10);
            float bright = light ? 100 - Math.min(20, power * 2) : 30;

            int rgb = Color.HSBtoRGB(hue, sat / 100.0f, bright / 100.0f);

            return String.format("BG_#%02X%02X%02X", (rgb >> 16) & 0xff, (rgb >> 8) & 0xff, rgb & 0xff);
        });
    }

    /** {@inheritDoc} */
    @Override public void format(LogEvent event, StringBuilder toAppendTo) {
        String bgColor = hideAnsi ? null : backgroundColor(lightConsole, nodeIndex());
        String fgColor = hideAnsi ? null : lightConsole ? "FG_#333333" : "FG_#b7b7b7";

        if (!hideAnsi && bgColor == null)
            toAppendTo.append(AnsiEscape.getDefaultStyle());

        for (int i = 0, size = patternFormatters.size(); i < size; i++) {
            if (bgColor != null) {
                toAppendTo.append(AnsiEscape.createSequence(bgColor));
                toAppendTo.append(AnsiEscape.createSequence(fgColor));
            }

            patternFormatters.get(i).format(event, toAppendTo);
        }

        if (!hideAnsi && bgColor == null)
            toAppendTo.append(AnsiEscape.getDefaultStyle());
    }
}
