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

package org.apache.ignite.springdata.misc;

import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.apache.ignite.springdata.repository.config.Query;
import org.apache.ignite.springdata.repository.config.RepositoryConfig;
import org.springframework.data.domain.AbstractPageRequest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;

/**
 *
 */
@RepositoryConfig(cacheName = "PersonCache")
public interface PersonSecondRepository extends IgniteRepository<Person, Integer> {
    /** */
    public Slice<Cache.Entry<Integer, Person>> findBySecondNameIsNot(String val, PageRequest pageReq);

    /** */
    @Query("SELECT _key, secondName FROM Person WHERE firstName REGEXP ?")
    public Slice<List> querySliceOfList(String val, AbstractPageRequest pageReq);
}
