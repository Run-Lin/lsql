#!/usr/bin/env bash
if [ -f "${SPARK_HOME}/capri-env.sh" ]; then
  # Promote all variable declarations to environment (exported) variables
  set -a
  . "${SPARK_HOME}/capri-env.sh"
  set +a
fi
APP_HOME="$(cd "`dirname "$0"`/.."; pwd)"
BUILD_DIR="$APP_HOME/build"
if [ -z "$JAVA_HOME" ]; then
  if [ -z "$JAVA_HOME" ]; then
    if [ $(command -v java) ]; then
      # If java is in /usr/bin/java, we want /usr
      JAVA_HOME="$(dirname $(dirname $(which java)))"
    fi
  fi
fi
if [ -z "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set, cannot proceed."
  exit -1
fi
if [ ! "$(command -v mvn)" ] ; then
    echo -e "Specify the Maven command with the --mvn flag"
    exit -1;
fi

cd "$APP_HOME"
profile=capri-test
// 创建目录
mkdir "$BUILD_DIR"/${profile}

BUILD_COMMAND=(mvn -T 1C clean package -DskipTests -Dscala.version=2.11.8 -DmainClass=LivySQL -P${profile} $@)
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND[@]}\n"
"${BUILD_COMMAND[@]}"

# Make directories
rm -rf "$BUILD_DIR"ll
mkdir -p "$BUILD_DIR"
cat "$APP_HOME"/dev/run.sh "$APP_HOME"/target/*-with-dependencies.jar > "$BUILD_DIR"/${profile}/spark-sql && chmod +x "$BUILD_DIR"/${profile}/spark-sql
echo -e "Building success ..."

# spark-submit
BUILD_COMMAND=(mvn -T 1C clean package -DskipTests -Dscala.version=2.11.8 -DmainClass=LivySubmit -P${profile} $@)
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND[@]}\n"
"${BUILD_COMMAND[@]}"

rm -rf "$BUILD_DIR"ll
mkdir -p "$BUILD_DIR"
cat "$APP_HOME"/dev/run.sh "$APP_HOME"/target/*-with-dependencies.jar > "$BUILD_DIR"/${profile}/spark-submit && chmod +x "$BUILD_DIR"/${profile}/spark-submit
echo -e "Building success ..."

#spark-shell
BUILD_COMMAND=(mvn -T 1C clean package -DskipTests -Dscala.version=2.11.8 -DmainClass=LivyShell -P${profile} $@)
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND[@]}\n"
"${BUILD_COMMAND[@]}"

rm -rf "$BUILD_DIR"ll
mkdir -p "$BUILD_DIR"
cat "$APP_HOME"/dev/run.sh "$APP_HOME"/target/*-with-dependencies.jar > "$BUILD_DIR"/${profile}/spark-shell && chmod +x "$BUILD_DIR"/${profile}/spark-shell
echo -e "Building success ..."

#pyspark
BUILD_COMMAND=(mvn -T 1C clean package -DskipTests -Dscala.version=2.11.8 -DmainClass=LivyPyspark -P${profile} $@)
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND[@]}\n"
"${BUILD_COMMAND[@]}"

rm -rf "$BUILD_DIR"ll
mkdir -p "$BUILD_DIR"
cat "$APP_HOME"/dev/run.sh "$APP_HOME"/target/*-with-dependencies.jar > "$BUILD_DIR"/${profile}/pyspark && chmod +x "$BUILD_DIR"/${profile}/pyspark
echo -e "Building success ..."

