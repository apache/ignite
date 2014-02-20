#!/bin/php

<?php

// @php.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
