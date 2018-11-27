#!/usr/bin/env bash

set -e

mvn install:install-file -Dfile=./lib/rocksdb/rocksdbjni.jar -DgroupId=extraLibs -DartifactId=rocksdb -Dversion=1.0 -Dpackaging=jar
mvn install:install-file -Dfile=./lib/leveldb/leveldbjni-all-99-master-SNAPSHOT.jar -DgroupId=extraLibs -DartifactId=leveldbjni-all -Dversion=99-master-SNAPSHOT -Dpackaging=jar
mvn install:install-file -Dfile=./lib/kyotocabinet-1.24/kyotocabinet.jar -DgroupId=extraLibs -DartifactId=kyotocabinet -Dversion=1.24 -Dpackaging=jar