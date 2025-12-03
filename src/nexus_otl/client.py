from __future__ import print_function

import logging

import grpc
import metadata_pb2
import metadata_pb2_grpc

def run():
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    print("Requesting metadata ...")
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = metadata_pb2_grpc.OTLStub(channel)
        response = stub.get_metadata(metadata_pb2.MetadataRequest(
            keys=["/tuning_info/", "/iers/"]
        ))
    print("Metadata received:\n", response.data)


if __name__ == "__main__":
    logging.basicConfig()
    run()