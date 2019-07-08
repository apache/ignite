/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.config;

import javax.activation.MimeType;
import javax.mail.internet.MimeMessage;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.services.IMailService;
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

/** */
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
