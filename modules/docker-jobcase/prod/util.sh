#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Common shell functions for Jobcase Apache Ignite containers

LOG_DATE='date +%Y/%m/%d:%H:%M:%S'
LOG_FILE=${JOBCASE_LOGS}/entrypoint.log

#
# Append log message as INFO to log file.
# The log message gets written to stdout and log file.
#
function log_info {
  # Check log file exists. 
  # If the file does not exist it will be created.
  test -f ${LOG_FILE} || touch ${LOG_FILE}
  
  echo `$LOG_DATE`" INFO ${1}" | tee -a ${LOG_FILE}
}

#
# Append log message as ERROR to log fie.
# The log message gets written to stderr and log file.
#
function log_error {
  test -f ${LOG_FILE} || touch ${LOG_FILE}
	
  echo `$LOG_DATE`" ERROR ${1}" | tee -a ${LOG_FILE} 1>&2
}