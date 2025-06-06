

package org.apache.ignite.console.web.socket;

import java.util.UUID;
import org.apache.ignite.console.dto.Announcement;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *  Transition service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class TransitionServiceSelfTest {
    /** Transition service. */
    @Autowired
    private TransitionService transitionSrvc;

    /** Browsers service. */
    @MockBean
    private BrowsersService browsersSrvc;

    /** Announcement captor. */
    @Captor
    private ArgumentCaptor<WebSocketEvent<Announcement>> annCaptor;

    /** */
    @Test
    public void testSendAnnouncement() {
        Announcement ann = new Announcement(UUID.randomUUID(), "test", true);

        transitionSrvc.broadcastAnnouncement(ann);

        verify(browsersSrvc, times(1)).sendAnnouncement(annCaptor.capture());
    }
}
