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

namespace Apache\Ignite\Internal\Utils;

use Apache\Ignite\Internal\Binary\MessageBuffer;

/** Utility class for logging errors and debug messages. */
class Logger
{
    private static $debug = false;

    public static function isDebug(): bool
    {
        return Logger::$debug;
    }

    public static function setDebug(bool $debug): void
    {
        Logger::$debug = $debug;
    }

    public static function logDebug($data, ...$args): void
    {
        if (Logger::$debug) {
            echo(sprintf($data, ...$args) . PHP_EOL);
        }
    }

    public static function logError($data, ...$args): void
    {
        if (Logger::$debug) {
            echo(sprintf("ERROR: $data", ...$args) . PHP_EOL);
        }
    }

    public static function logBuffer(MessageBuffer $buffer, int $startPos = 0, int $length = -1): void
    {
        if (Logger::$debug) {
            if ($length < 0) {
                $length = $buffer->getLength();
            }
            $message = $buffer->getSlice($startPos, $length);
            Logger::logDebug('[' . implode(',', array_map('ord', str_split($message))) . ']');
        }
    }
}
