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

package org.apache.ignite.springdata.compoundkey;

import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.springdata20.repository.IgniteRepository;
import org.apache.ignite.springdata20.repository.config.RepositoryConfig;
import org.springframework.stereotype.Repository;

/** City repository */
@Repository
@RepositoryConfig(cacheName = "City", autoCreateCache = true)
public interface CityRepository extends IgniteRepository<City, CityKey> {
    /**
     * Find city by id
     * @param id city identifier
     * @return city
     * */
    public City findById(int id);

    /**
     * Find all cities by coutrycode
     * @param cc coutrycode
     * @return list of cache enrties CityKey -> City
     * */
    public List<Cache.Entry<CityKey, City>> findByCountryCode(String cc);
}
