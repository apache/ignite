#!/usr/bin/env bash
set -o nounset; set -o errexit; set -o pipefail; set -o errtrace; set -o functrace


#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


## VARS ##
PACKAGE_VERSION="${1}"

RPM_WORK_DIR="/tmp/apache-ignite-rpm"



## START ##
mkdir -pv ${RPM_WORK_DIR}/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
cp -rfv ignite ${RPM_WORK_DIR}/BUILD/
cp -rfv apache-ignite.spec ${RPM_WORK_DIR}/SPECS/
sed -r "4 i if [ \"\$(whoami)\" != \"ignite\" ]; then echo \"Ignite CLI can only be run by 'ignite' user.\"; echo \"Swith user to ignite by executing '(sudo) su ignite'\"; exit 1; fi" \
    -i ${RPM_WORK_DIR}/BUILD/ignite
rpmbuild -bb \
         --define "_topdir ${RPM_WORK_DIR}" \
         ${RPM_WORK_DIR}/SPECS/apache-ignite.spec
cp -rfv ${RPM_WORK_DIR}/RPMS/noarch/apache-ignite-${PACKAGE_VERSION}.noarch.rpm ./

