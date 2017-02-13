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

export default [() => {
    class AclRoute {
        static checkAccess(permissions, failState) {
            failState = failState || '403';

            return ['$q', '$state', 'AclService', 'User', 'IgniteActivitiesData', function($q, $state, AclService, User, Activities) {
                const action = this.name ? $state.href(this.name) : null;

                return User.read()
                    .catch(() => {
                        User.clean();

                        if ($state.current.name !== 'signin')
                            $state.go('signin');

                        return $q.reject('Failed to detect user');
                    })
                    .then(() => {
                        if (AclService.can(permissions))
                            return Activities.post({ action });

                        $state.go(failState);

                        return $q.reject('User are not authorized');
                    });
            }];
        }

        static $get() {
            return AclRoute;
        }
    }

    return AclRoute;
}];
