# Benthos custom UDP input plugin

This custom benthos plugin starts a UDP server to listen to UDP
and set the metadata `source_address` with the original client's address url and port
It can be configured through the following env vars:

| Env Var                                    | Description | Default value          |
|--------------------------------------------|-------------|------------------------|
| HTTP_ADDRESS                               |             | 0.0.0.0:4195           |

