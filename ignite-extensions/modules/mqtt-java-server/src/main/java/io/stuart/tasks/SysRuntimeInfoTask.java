/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.tasks;

import java.util.TimerTask;

import io.stuart.services.cache.CacheService;

public class SysRuntimeInfoTask extends TimerTask {

    private CacheService cacheService;

    public SysRuntimeInfoTask(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public void run() {
        cacheService.updateNodeSysRuntimeInfo();
    }

}
