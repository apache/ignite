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
