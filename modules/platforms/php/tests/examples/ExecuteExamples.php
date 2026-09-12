<?php
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

namespace Apache\Ignite\Tests;

use PHPUnit\Framework\TestCase;

final class ExecuteExamplesTestCase extends TestCase
{
    public function testCachePutGetExample(): void
    {
        TestingHelper::executeExample('CachePutGetExample.php', $this, array($this, 'checkNoErrors'));
    }

    public function testSqlExample(): void
    {
        TestingHelper::executeExample('SqlExample.php', $this, array($this, 'checkNoErrors'));
    }

    public function testSqlQueryEntriesExample(): void
    {
        TestingHelper::executeExample('SqlQueryEntriesExample.php', $this, array($this, 'checkNoErrors'));
    }

    public function testFailoverExample(): void
    {
        TestingHelper::executeExample('FailoverExample.php', $this, array($this, 'checkClientConnected'));
    }

    public function checkNoErrors(array $output): void
    {
        foreach ($output as $out) {
            $this->assertNotContains('ERROR:', $out);
        }
    }

    public function checkClientConnected(array $output): void
    {
        foreach ($output as $out) {
            if (strpos($out, 'Client connected') !== false) {
                return;
            }
        }
        $this->assertTrue(false);
    }
}
