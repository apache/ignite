package org.apache.ignite.configuration;

import java.util.regex.Pattern;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;

/**
 * Test covers cases where Ignite configuration,
 * or it's nested objects has default implementation of {@link Object#toString()}
 */
@WithSystemProperty(key = IGNITE_QUIET, value = "false")
public class IgniteConfigurationTest extends GridCommonAbstractTest {
    /** Error message to be prompted */
    private static final String ASSERTION_ERROR_MESSAGE =
            "Ignite configuration log message contains objects with default toString implementation!";

    /** Pattern to check any object has default {@link Object#toString()} implementation */
    private static final Pattern ERROR_PATTERN = Pattern.compile("^(?=.*IgniteConfiguration \\[)(?!.*@\\d+).*$");

    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /**
     * Check ignite configuration log message contains no @ letters
     * It's a common way to ensure all objects in prompt has {@link Object#toString()} overriden
     */
    @Test
    public void testIgniteConfigurationPrompt() throws Exception {
        LogListener igniteConfigurationLogListener = LogListener
                .matches(ERROR_PATTERN)
                .atLeast(1)
                .build();
        listeningLog.registerListener(igniteConfigurationLogListener);
        try (IgniteEx ignored = startGrid(0)) {
            Assert.assertTrue(ASSERTION_ERROR_MESSAGE, igniteConfigurationLogListener.check());
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
                .setGridLogger(listeningLog);
    }
}
