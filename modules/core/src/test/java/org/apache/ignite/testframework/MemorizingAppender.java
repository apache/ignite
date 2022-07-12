/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testframework;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

/**
 * A Log4j {@link org.apache.log4j.Appender} that memorizes all the events it gets from loggers. These events are made
 * available to the class users.
 */
public class MemorizingAppender extends AppenderSkeleton {
    /**
     * Events that were seen by this Appender.
     */
    private final List<LoggingEvent> events = new CopyOnWriteArrayList<>();

    /** {@inheritDoc} */
    @Override protected void append(LoggingEvent event) {
        events.add(event);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public boolean requiresLayout() {
        return false;
    }

    /**
     * Returns all events that were seen by this Appender so far.
     *
     * @return All events that were seen by this Appender so far.
     */
    public List<LoggingEvent> events() {
        return new ArrayList<>(events);
    }

    /**
     * Adds this Appender to the logger corresponding to the provided class.
     *
     * @param target Class on whose logger to install this Appender.
     */
    public void installSelfOn(Class<?> target) {
        Logger logger = Logger.getLogger(target);

        logger.addAppender(this);
    }

    /**
     * Removes this Appender from the logger corresponding to the provided class.
     *
     * @param target Class from whose logger to remove this Appender.
     */
    public void removeSelfFrom(Class<?> target) {
        Logger logger = Logger.getLogger(target);

        logger.removeAppender(this);
    }

    /**
     * Returns the single event satisfying the given predicate. If no such event exists or more than one event matches,
     * then an exception is thrown.
     *
     * @param predicate Predicate to use to select the event.
     * @return The single event satisfying the given predicate.
     */
    public LoggingEvent singleEventSatisfying(Predicate<LoggingEvent> predicate) {
        List<LoggingEvent> matches = events.stream().filter(predicate).collect(toList());

        assertThat(matches, hasSize(1));

        return matches.get(0);
    }
}
