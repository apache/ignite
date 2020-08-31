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
package org.apache.ignite.springdata.misc;

import java.util.Collection;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.springdata20.repository.IgniteRepository;
import org.apache.ignite.springdata20.repository.config.Query;
import org.apache.ignite.springdata20.repository.config.RepositoryConfig;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.Param;

/**
 * Test repository.
 */
@RepositoryConfig(cacheName = "PersonCache")
public interface PersonRepository extends IgniteRepository<Person, Integer> {
    /** */
    public List<Person> findByFirstName(String val);

    /** */
    @Query("firstName = ?")
    public List<PersonProjection> queryByFirstNameWithProjection(String val);

    /** */
    @Query("firstName = :firstname")
    public List<PersonProjection> queryByFirstNameWithProjectionNamedParameter(@Param("firstname") String val);

    /** */
    @Query("firstName = :firstname")
    public <P> List<P> queryByFirstNameWithProjectionNamedParameter(Class<P> dynamicProjection, @Param("firstname") String val);

    /** */
    @Query("firstName = :firstname")
    public <P> P queryOneByFirstNameWithProjectionNamedParameter(Class<P> dynamicProjection, @Param("firstname") String val);

    /** */
    @Query("firstName = ?#{[1]}")
    public List<PersonProjection> queryByFirstNameWithProjectionNamedIndexedParameter(@Param("notUsed") String notUsed, @Param("firstname") String val);

    /** */
    @Query(textQuery = true, value = "#{#firstname}", limit = 2)
    public List<PersonProjection> textQueryByFirstNameWithProjectionNamedParameter(@Param("firstname") String val);

    @Query(value = "select * from (sElecT * from #{#entityName} where firstName = :firstname)", forceFieldsQuery = true)
    public List<PersonProjection> queryByFirstNameWithProjectionNamedParameterAndTemplateDomainEntityVariable(@Param("firstname") String val);

    @Query(value = "firstName = ?#{sampleExtension.transformParam(#firstname)}")
    public List<PersonProjection> queryByFirstNameWithProjectionNamedParameterWithSpELExtension(@Param("firstname") String val);

    /** */
    public List<Person> findByFirstNameContaining(String val);

    /** */
    public List<Person> findByFirstNameRegex(String val, Pageable pageable);

    /** */
    public Collection<Person> findTopByFirstNameContaining(String val);

    /** */
    public Iterable<Person> findFirst10ByFirstNameLike(String val);

    /** */
    public int countByFirstName(String val);

    /** */
    public int countByFirstNameLike(String val);

    /** */
    public int countByFirstNameLikeAndSecondNameLike(String like1, String like2);

    /** */
    public int countByFirstNameStartingWithOrSecondNameStartingWith(String like1, String like2);

    /** */
    public List<Cache.Entry<Integer, Person>> findBySecondNameLike(String val);

    /** */
    public Cache.Entry<Integer, Person> findTopBySecondNameLike(String val);

    /** */
    public PersonProjection findTopBySecondNameStartingWith(String val);

    /** */
    @Query("firstName = ?")
    public List<Person> simpleQuery(String val);

    /** */
    @Query("firstName REGEXP ?")
    public List<Person> queryWithSort(String val, Sort sort);

    /** */
    @Query("SELECT * FROM Person WHERE firstName REGEXP ?")
    public List<Person> queryWithPageable(String val, Pageable pageable);

    /** */
    @Query("SELECT secondName FROM Person WHERE firstName REGEXP ?")
    public List<String> selectField(String val, Pageable pageable);

    /** */
    @Query("SELECT _key, secondName FROM Person WHERE firstName REGEXP ?")
    public List<List> selectSeveralField(String val, Pageable pageable);

    /** */
    @Query("SELECT count(1) FROM (SELECT DISTINCT secondName FROM Person WHERE firstName REGEXP ?)")
    public int countQuery(String val);

    /** Top 3 query */
    public List<Person> findTop3ByFirstName(String val);

    /** Delete query */
    public long deleteByFirstName(String firstName);

    /** Remove Query */
    public List<Person> removeByFirstName(String firstName);

    /** Delete using @Query with keyword in lower-case */
    @Query("delete FROM Person WHERE secondName = ?")
    public void deleteBySecondNameLowerCase(String secondName);

    /** Delete using @Query but with errors on the query */
    @Query("DELETE FROM Person WHERE firstName = ? AND ERRORS = 'ERRORS'")
    public void deleteWrongByFirstNameQuery(String firstName);

    /** Update using @Query with keyword in mixed-case */
    @Query("upDATE Person SET secondName = ? WHERE firstName = ?")
    public int setFixedSecondNameMixedCase(String secondName, String firstName);

    /** Update using @Query but with errors on the query */
    @Query("UPDATE Person SET secondName = ? WHERE firstName = ? AND ERRORS = 'ERRORS'")
    public int setWrongFixedSecondName(String secondName, String firstName);
}
