# Protobuf issues with Erlang
Adding a new protobuf message:
* `make shell` in Erlang root folder.
* Change directory to target folder for auto generated files.
* `grpc:compile(<file>).`
* Copy and paste **previous services** into the generated file as it is going to get override. 