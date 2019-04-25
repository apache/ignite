#!/bin/php

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

/**
 * PHP client API.
 */
interface GridClientData {
    /**
     * @abstract
     * @return string
     */
    public function cacheName();

    /**
     * @abstract
     * @param string $nodeId
     * @return GridClientData
     */
    public function pinNode(string $nodeId);

    /**
     * @abstract
     * @return GridClientNode|null
     */
    public function pinnedNode();

    /**
     * @abstract
     * @param mixed $key
     * @param mixed $val
     * @return boolean
     */
    public function put($key, $val);

    /**
     * @abstract
     * @param array $keys
     * @param array $values
     * @return void
     */
    public function putAll(array $keys, array $values);

    /**
     * @abstract
     * @param $key
     * @return mixed
     */
    public function get($key);

    /**
     * @abstract
     * @param array $keys
     * @return array
     */
    public function getAll(array $keys);

    /**
     * @abstract
     * @param mixed $key
     * @return boolean
     */
    public function remove($key);

    /**
     * @abstract
     * @param array|null $keys
     * @return void
     */
    public function removeAll(array $keys = null);

    /**
     * @abstract
     * @param mixed $key
     * @param mixed $val
     * @return boolean
     */
    public function replace($key, $val);

    /**
     * @abstract
     * @param mixed $key
     * @param mixed $val1
     * @param mixed $val2
     * @return boolean
     */
    public function cas($key, $val1, $val2);

    /**
     * @abstract
     * @param mixed $key
     * @return string
     */
    public function affinity($key);

    /**
     * @abstract
     * @param mixed|null $key
     * @return array
     */
    public function metrics($key = null);
}

?>
