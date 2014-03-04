#!/bin/php

<?php

/* @php.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

/**
 * PHP client API.
 */
interface GridClient {
    /**
     * @abstract
     * @param string|null $cacheName
     * @return GridClientData
     */
    public function data(string $cacheName = null);

    /**
     * @abstract
     * @return GridClientCompute
     */
    public function compute();
}

?>
