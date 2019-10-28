#!/bin/bash
set -ev
cd shared_scala
sbt ++$TRAVIS_SCALA_VERSION test
sbt ++$TRAVIS_SCALA_VERSION publishLocal
cd ..
cd runner
sbt ++$TRAVIS_SCALA_VERSION test
sbt ++$TRAVIS_SCALA_VERSION publishLocal
cd ..
cd kompics
sbt ++$TRAVIS_SCALA_VERSION test
cd ..
cd akka
sbt ++$TRAVIS_SCALA_VERSION test
cd ..
