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

package org.apache.ignite.console.services;

import org.apache.ignite.console.MockConfiguration;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.event.Event;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.repositories.ActivitiesRepository;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

import static org.apache.ignite.console.event.ActivityEventType.ACTIVITY_UPDATE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Acctivities service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {MockConfiguration.class})
public class ActivitiesServiceTest {
    /** Activities service. */
    @Autowired
    private ActivitiesService activitiesSrvc;

    /** Event publisher. */
    @MockBean
    private EventPublisher evtPublisher;

    /** Activities repository. */
    @MockBean
    private ActivitiesRepository activitiesRepo;

    /**
     * Should publish event with ACTIVITY_UPDATE type.
     */
    @Test
    public void shouldPublishActivityUpdateEvent() {
        when(activitiesRepo.save(any(UUID.class), anyString(), anyString()))
            .thenAnswer(invocation -> new Activity());

        activitiesSrvc.save(UUID.randomUUID(), "grp", "act");

        ArgumentCaptor<Event> captor = ArgumentCaptor.forClass(Event.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals(ACTIVITY_UPDATE, captor.getValue().getType());
        Assert.assertTrue(captor.getValue().getSource() instanceof Activity);
    }
}
