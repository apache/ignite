<?php
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

namespace Apache\Ignite\Query;

/**
 * Base class representing an GridGain SQL or Scan query.
 *
 * The class is abstract, only subclasses may be instantiated.
 */
abstract class Query
{
    const PAGE_SIZE_DEFAULT = 1024;

    protected $local;
    protected $operation;
    protected $pageSize;

    protected function __construct(int $operation)
    {
        $this->operation = $operation;
        $this->local = false;
        $this->pageSize = Query::PAGE_SIZE_DEFAULT;
    }

    /**
     * Set local query flag.
     *
     * @param bool $local local query flag: true or false.
     * @return Query the same instance of the Query.
     */
    public function setLocal(bool $local): Query
    {
        $this->local = $local;
        return $this;
    }

    /**
     * Set cursor page size.
     *
     * @param int $pageSize cursor page size.
     * @return Query the same instance of the Query.
     */
    public function setPageSize(int $pageSize): Query
    {
        $this->pageSize = $pageSize;
        return $this;
    }

    // This is not the public API method, is not intended for usage by an application.
    public function getOperation(): int
    {
        return $this->operation;
    }
}
