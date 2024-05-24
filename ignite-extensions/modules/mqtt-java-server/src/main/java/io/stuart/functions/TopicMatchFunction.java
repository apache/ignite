/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.functions;

import org.apache.ignite.cache.query.annotations.QuerySqlFunction;

import io.stuart.consts.TopicConst;
import io.stuart.utils.TopicUtil;

public class TopicMatchFunction {

    @QuerySqlFunction
    public static int match(String topic, String filter) {
        int result = 1;

        if (!TopicUtil.validateTopic(topic) || !TopicUtil.validateTopic(filter)) {
            return 0;
        }

        if ("#".equals(filter)) {
            return 1;
        }

        String[] topicWords = TopicUtil.words(topic);
        String[] filterWords = TopicUtil.words(filter);

        int topicWordLength = topicWords.length;
        int filterWordLength = filterWords.length;

        if (topicWordLength > filterWordLength) {
            if (!TopicConst.POUND.equals(filterWords[filterWordLength - 1])) {
                result = 0;
            } else {
                result = loopMatch(topicWords, filterWords, filterWordLength);
            }
        } else if (topicWordLength == filterWordLength) {
            result = loopMatch(topicWords, filterWords, topicWordLength);
        } else {
            if (filterWordLength - topicWordLength >= 2) {
                result = 0;
            } else if (!TopicConst.POUND.equals(filterWords[topicWordLength])) {
                result = 0;
            } else {
                result = loopMatch(topicWords, filterWords, topicWordLength);
            }
        }

        return result;
    }

    @QuerySqlFunction
    public static int rmatch(String topic, String filter) {
        int result = 1;

        if (!TopicUtil.validateTopic(topic) || !TopicUtil.validateTopic(filter)) {
            return 0;
        }

        if ("#".equals(topic)) {
            return 1;
        }

        String[] topicWords = TopicUtil.words(topic);
        String[] filterWords = TopicUtil.words(filter);

        int topicWordLength = topicWords.length;
        int filterWordLength = filterWords.length;

        if (topicWordLength > filterWordLength) {
            if (topicWordLength - filterWordLength >= 2) {
                result = 0;
            } else if (!TopicConst.POUND.equals(topicWords[filterWordLength])) {
                result = 0;
            } else {
                result = rloopMatch(topicWords, filterWords, filterWordLength);
            }
        } else if (topicWordLength == filterWordLength) {
            result = rloopMatch(topicWords, filterWords, topicWordLength);
        } else {
            if (!TopicConst.POUND.equals(topicWords[topicWordLength - 1])) {
                result = 0;
            } else {
                result = rloopMatch(topicWords, filterWords, topicWordLength);
            }
        }

        return result;
    }

    private static int loopMatch(String[] topic, String[] filter, int length) {
        int result = 1;

        String tWord = null;
        String fWord = null;

        for (int i = 0; i < length; ++i) {
            tWord = topic[i];
            fWord = filter[i];

            if (!tWord.equals(fWord) && !TopicConst.PLUS.equals(fWord) && !TopicConst.POUND.equals(fWord)) {
                result = 0;
                break;
            }
        }

        return result;
    }

    private static int rloopMatch(String[] topic, String[] filter, int length) {
        int result = 1;

        String tWord = null;
        String fWord = null;

        for (int i = 0; i < length; ++i) {
            tWord = topic[i];
            fWord = filter[i];

            if (!tWord.equals(fWord) && !TopicConst.PLUS.equals(tWord) && !TopicConst.POUND.equals(tWord)) {
                result = 0;
                break;
            }
        }

        return result;
    }

}
