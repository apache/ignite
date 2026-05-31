

package org.apache.ignite.console.common;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

/**
 * Marker interface that indicates that component should be activated in test profile.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Primary
@Profile("test")
public @interface Test {
}
