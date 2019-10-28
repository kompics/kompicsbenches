<<<<<<< HEAD
# Protobuf issues with Erlang
Adding a new protobuf message:
* `make shell` in Erlang root folder.
* Change directory to target folder for auto generated files (`cd("src").`).
* `grpc:compile(<file>).`
* Copy and paste **previous services** into the generated file as it is going to get override.
=======
# Protobuf issues with Erlang
Adding a new protobuf message:
* `make shell` in Erlang root folder.
* Change directory to target folder for auto generated files (`cd("src").`).
* `grpc:compile(<file>).`
* Copy and paste **previous services** into the generated file as it is going to get override.
>>>>>>> c92c44604e519dd0b6cc96f7ef122b9ca6b9cde1
