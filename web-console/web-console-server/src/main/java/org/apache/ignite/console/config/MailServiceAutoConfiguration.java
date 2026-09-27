

package org.apache.ignite.console.config;

import jakarta.activation.MimeType;
import jakarta.mail.internet.MimeMessage;

import org.apache.ignite.console.agent.service.IMailService;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.services.MailService;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.mail.MailSender;
import org.springframework.mail.javamail.JavaMailSender;

/**
 * Mail service auto configuration.
 */
@Configuration
@Conditional(MailServiceAutoConfiguration.MailSenderCondition.class)
@ConditionalOnClass({ MimeMessage.class, MimeType.class, MailSender.class, MailService.class })
@EnableConfigurationProperties(MailPropertiesEx.class)
public class MailServiceAutoConfiguration {
    /** JavaMail sender. */
    private JavaMailSender mailSnd;

    /** Message properties. */
    private MailPropertiesEx props;

    /**
     * Condition to trigger the creation of a {@link MailSender}. This kicks in if either
     * the host or jndi name property is set.
     */
    static class MailSenderCondition extends AnyNestedCondition {
        /**
         * Default constructor.
         */
        MailSenderCondition() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        /** */
        @ConditionalOnProperty(prefix = "spring.mail", name = "host")
        static class HostProperty {
            // No-op.
        }

        /** */
        @ConditionalOnProperty(prefix = "spring.mail", name = "jndi-name")
        static class JndiNameProperty {
            // No-op.
        }
    }

    /**
     * @param mailSnd Mail send.
     * @param props Properties.
     */
    public MailServiceAutoConfiguration(JavaMailSender mailSnd, MailPropertiesEx props) {
        this.mailSnd = mailSnd;
        this.props = props;
    }

    /** Mail sending service. */
    @Bean
    @Primary
    public IMailService mailService() {
        return new MailService(WebConsoleMessageSource.getAccessor(), mailSnd, props);
    }
}
