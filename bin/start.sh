#!/usr/bin/env bash
APP_HOME="$(cd "`dirname "$0"`/.."; pwd)"
MAIN_CLASS="com.r.spark.repl.sql.LivyCli"
cd "$APP_HOME/build"
java -cp *.jar $MAIN_CLASS $@