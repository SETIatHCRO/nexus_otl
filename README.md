# Observatory Transactional Layer


## GRPC compilation

`/src/nexus_otl$ python -m grpc_tools.protoc -I../../proto --python_out=. --pyi_out=. --grpc_python_out=. ../../proto/metadata.proto`