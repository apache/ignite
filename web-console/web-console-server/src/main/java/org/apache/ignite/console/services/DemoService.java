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

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.repositories.ConfigurationsRepository;
import org.apache.ignite.console.web.model.ConfigurationKey;
import org.apache.ignite.internal.util.typedef.F;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.console.utils.Utils.asJson;
import static org.apache.ignite.console.utils.Utils.fromJson;

/**
 * Service to handle configurations.
 */
@Service
public class DemoService {
    /** */
    private static final Logger log = LoggerFactory.getLogger(DemoService.class);

    /** Demo clusters. */
    private List<JsonObject> clusters;

    /** Repository to work with configurations. */
    private final ConfigurationsRepository cfgsRepo;

    /**
     * @param cfgsRepo Configurations repository.
     */
    public DemoService(ConfigurationsRepository cfgsRepo) {
        this.cfgsRepo = cfgsRepo;
    }

    /**
     * @param json Json.
     * @param prop Property.
     */
    private List<Object> fillId(JsonObject json, String prop) {
        List<JsonObject> items = json.getJsonArray(prop).stream()
            .map(c -> {
                JsonObject obj = asJson(c);

                obj.put("id", UUID.randomUUID().toString());

                return obj;
            }).collect(toList());

        json.put(prop, items);

        return items.stream().map(i -> i.get("id")).collect(toList());
    }

    /**
     * @param json Json.
     */
    private void linkModelsWithCaches(JsonObject json, List<Object> cacheIds) {
        List<JsonObject> items = json.getJsonArray("models").stream()
            .map(c -> {
                JsonObject obj = asJson(c);

                obj.put("caches", Collections.singleton(cacheIds.remove(0)));

                return obj;
            }).collect(toList());

        json.put("models", items);
    }

    /**
     * @param accId Account ID.
     */
    public void reset(UUID accId) {
        ConfigurationKey space = new ConfigurationKey(accId, true);

        cfgsRepo.deleteByAccountId(space);

        if (F.isEmpty(clusters)) {
            try {
                ClassPathResource res = new ClassPathResource("demo-clusters.json");

                String content = FileCopyUtils.copyToString(new InputStreamReader(res.getInputStream(), UTF_8));

                clusters = fromJson(content, new TypeReference<List<JsonObject>>() { });
            }
            catch (Exception e) {
                log.error("Failed to get demo clusters", e);
            }
        }

        if (!F.isEmpty(clusters)) {
            for (JsonObject json : clusters) {
                JsonObject cluster = json.getJsonObject("cluster");

                cluster.put("id", UUID.randomUUID().toString());

                List<Object> mdlIds = fillId(json, "models");
                List<Object> cacheIds = fillId(json, "caches");

                cluster.put("models", mdlIds);
                cluster.put("caches", cacheIds);

                linkModelsWithCaches(json, new ArrayList<>(cacheIds));

                json.put("cluster", cluster);

                cfgsRepo.saveAdvancedCluster(space, json);
            }
        }
    }
}
