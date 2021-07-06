# Shell script for installation of dependencies on Ubuntu 20.04

# Add Erlang and Sbt sources
wget -O- https://packages.erlang-solutions.com/ubuntu/erlang_solutions.asc | sudo apt-key add -
echo "deb https://packages.erlang-solutions.com/ubuntu focal contrib" | sudo tee /etc/apt/sources.list.d/rabbitmq.list
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add

# Builders
sudo apt-get update
sudo apt install make
sudo apt install build-essential

# JDK
sudo apt-get install default-jdk -y

# Scala
sudo wget https://downloads.lightbend.com/scala/2.13.0/scala-2.13.0.deb
sudo dpkg -i scala-2.13.0

# Sbt
sudo apt-get install sbt

# Erlang
sudo apt install erlang

# Rustup
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain nightly
source $HOME/.cargo/env


# Ammonite
sudo sh -c '(echo "#!/usr/bin/env sh" && curl -L https://github.com/com-lihaoyi/Ammonite/releases/download/2.3.8/2.12-2.3.8) > /usr/local/bin/amm && chmod +x /usr/local/bin/amm'

# Protobuf
./scripts/travis_install_protobuf.sh