

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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Activities service test.
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
