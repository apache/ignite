package org.apache.ignite.springdata.misc;

import org.springframework.beans.factory.annotation.Value;

/**
 * Advanced SpEl Expressions into projection
 *
 * @author Manuel Núñez Sánchez (manuel.nunez@hawkore.com)
 */
public interface FullNameProjection {

    /**
     * Sample of using SpEL expression
     * @return
     */
    @Value("#{target.firstName + ' ' + target.secondName}")
    String getFullName();
}
