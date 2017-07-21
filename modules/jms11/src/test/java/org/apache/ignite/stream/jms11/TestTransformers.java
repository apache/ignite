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

package org.apache.ignite.stream.jms11;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

/**
 * Test transformers for JmsStreamer tests.
 *
 * @author Raul Kripalani
 */
public class TestTransformers {

    /**
     * Returns a transformer for JMS {@link TextMessage}s, capable of extracting many tuples from a single message,
     * if pipe characters are encountered.
     *
     * @return
     */
    public static MessageTransformer<TextMessage, String, String> forTextMessage() {
        return new MessageTransformer<TextMessage, String, String>() {
            @Override
            public Map<String, String> apply(TextMessage message) {
                final Map<String, String> answer = new HashMap<>();
                String text;
                try {
                    text = message.getText();
                }
                catch (JMSException e) {
                    e.printStackTrace();
                    return Collections.emptyMap();
                }
                for (String s : text.split("\\|")) {
                    String[] tokens = s.split(",");
                    answer.put(tokens[0], tokens[1]);
                }
                return answer;
            }
        };
    }

    /**
     * Returns a transformer for JMS {@link ObjectMessage}s, capable of extracting many tuples from a single message,
     * if the payload is a {@link Collection}.
     *
     * @return
     */
    public static MessageTransformer<ObjectMessage, String, String> forObjectMessage() {
        return new MessageTransformer<ObjectMessage, String, String>() {
            @Override @SuppressWarnings("unchecked")
            public Map<String, String> apply(ObjectMessage message) {
                Object object;
                try {
                    object = message.getObject();
                }
                catch (JMSException e) {
                    e.printStackTrace();
                    return Collections.emptyMap();
                }

                final Map<String, String> answer = new HashMap<>();
                if (object instanceof Collection) {
                    for (TestObject to : (Collection<TestObject>)object)
                        answer.put(to.getKey(), to.getValue());

                }
                else if (object instanceof TestObject) {
                    TestObject to = (TestObject)object;
                    answer.put(to.getKey(), to.getValue());
                }
                return answer;
            }
        };
    }

    public static MessageTransformer<TextMessage, String, String> generateNoEntries() {
        return new MessageTransformer<TextMessage, String, String>() {
            @Override
            public Map<String, String> apply(TextMessage message) {
                return null;
            }
        };
    }

    public static class TestObject implements Serializable {
        private static final long serialVersionUID = -7332027566186690945L;

        private String key;
        private String value;

        public TestObject(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

    }

}