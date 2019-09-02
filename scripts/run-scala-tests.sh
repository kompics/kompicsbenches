#!/bin/bash
set -ev
cd shared_scala
exec sbt ++$TRAVIS_SCALA_VERSION test
exec sbt ++$TRAVIS_SCALA_VERSION publishLocal
cd ..
cd runner
exec sbt ++$TRAVIS_SCALA_VERSION test
exec sbt ++$TRAVIS_SCALA_VERSION publishLocal
cd ..
cd kompics
exec sbt ++$TRAVIS_SCALA_VERSION test
cd ..
cd akka
exec sbt ++$TRAVIS_SCALA_VERSION test
cd ..
