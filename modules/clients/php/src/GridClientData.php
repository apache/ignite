#!/bin/php

<?php

/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
