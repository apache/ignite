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

package org.apache.ignite.agent.config;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.ignite.internal.util.typedef.F;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptorAdapter;

import static org.apache.ignite.agent.utils.AgentObjectMapperFactory.binaryMapper;
import static org.apache.ignite.agent.utils.AgentUtils.fromNullableCollection;
import static org.springframework.messaging.simp.SimpMessageHeaderAccessor.getDestination;

/**
 * Interceptor which handle all messages and save them in map.
 */
public class TestChannelInterceptor extends ChannelInterceptorAdapter {
    /** Object mapper. */
    private ObjectMapper mapper = binaryMapper();

    /** Messages. */
    private Map<String, List<Object>> messages = new ConcurrentHashMap<>();

    /** Subscribed destinations. */
    private Set<String> subscribedDests = ConcurrentHashMap.newKeySet();

    /** {@inheritDoc} */
    @Override public Message<?> preSend(Message<?> msg, MessageChannel ch) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(msg);

        if (accessor.getCommand() == StompCommand.SEND) {
            String dest = getDestination(msg.getHeaders());

            messages.compute(dest, (k, v) -> {
                if (v == null)
                    return Lists.newArrayList(msg.getPayload());

                v.add(msg.getPayload());

                return v;
            });
        }

        if (accessor.getCommand() == StompCommand.SUBSCRIBE)
            subscribedDests.add(getDestination(msg.getHeaders()));

        return msg;
    }

    /**
     * @param dest Destination.
     */
    public boolean isSubscribedOn(String dest) {
        return subscribedDests.contains(dest);
    }

    /**
     * @param dest Destination.
     */
    public <T> List<T> getAllPayloads(String dest, Class<T> clazz) {
        return fromNullableCollection(messages.get(dest)).map(p -> mapPayload(p, clazz)).collect(Collectors.toList());
    }

    /**
     * @param dest Destination.
     */
    public <T> T getPayload(String dest, Class<T> clazz) {
        Object payload = getPayload(dest);

        return mapPayload(payload, clazz);
    }

    /**
     * @param dest Destination.
     */
    public <T> List<T> getListPayload(String dest, Class<T> clazz) {
        Object payload = F.last(messages.get(dest));

        if (payload == null)
            return null;

        JavaType type = mapper.getTypeFactory().constructCollectionType(List.class, clazz);

        try {
            return mapper.readValue((byte[]) payload, type);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param dest Destination.
     */
    public Object getPayload(String dest) {
        return F.last(messages.get(dest));
    }

    /**
     * @param dest Destination.
     */
    public List<Object> getAllRawPayloads(String dest) {
        return messages.get(dest);
    }

    /**
     * @param payload Payload.
     * @param clazz Clazz.
     */
    private <T> T mapPayload(Object payload, Class<T> clazz) {
        if (payload == null)
            return null;

        try {
            return mapper.readValue((byte[]) payload, clazz);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
