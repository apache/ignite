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
interface GridClientDataAffinity {
    /**
     * @abstract
     * @param $key
     * @param array $nodes
     * @return GridClientNode
     */
    public function node($key, array $nodes);
}

?>
