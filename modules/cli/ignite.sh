#!/usr/bin/env bash
set -o nounset; set -o errexit; set -o pipefail; set -o errtrace; set -o functrace

MYSELF=`which "${0}" 2>/dev/null`
[ $? -gt 0 -a -f "${0}" ] && MYSELF="./${0}"
java=java
if test -n "${JAVA_HOME:-}"; then
    java="${JAVA_HOME}/bin/java"
fi
exec "${java}" ${java_args:-} -jar ${MYSELF} "$@"
exit 1
