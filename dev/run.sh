#!/usr/bin/env bash
CUR_DIR=`pwd`
if [ -f "${CUR_DIR}/capri-env.sh" ]; then
  # Promote all variable declarations to environment (exported) variables
  set -a
  . "${CUR_DIR}/capri-env.sh"
  set +a
elif [ -f "${SPARK_HOME}/capri-env.sh" ]; then
  set -a
  . "${SPARK_HOME}/capri-env.sh"
  set +a
else
  set -a
  . "${SPARK2_HOME}/capri-env.sh"
  set +a
fi
MY_RUN=`which "$0" 2>/dev/null`
[ $? -gt 0 -a -f "$0" ] && MY_RUN="./$0"
java=java
if test -n "$JAVA_HOME"; then
    java="$JAVA_HOME/bin/java"
else
  echo "Error: JAVA_HOME is not set."
  exit 1
fi
if test -z "$JAVA_OPTS"; then
   JAVA_OPTS="-Xms128m -Xmx1024m"
fi
exec "$java" $JAVA_OPTS -jar $MY_RUN "$@"
exit 1