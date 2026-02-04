from __future__ import print_function

import logging
import time

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
        start_time = time.time()
        response = stub.get_metadata(metadata_pb2.MetadataRequest(
            keys=["/v1/observatory", "/v1/observation/iers"]
        ))
        stop_time = time.time()
    keys = list(response.data.keys())
    keys.sort()
    print("Metadata received:\n", {
        k: response.data[k]
        for k in keys
    })
    print(f"\n... {len(keys)} scalars in {stop_time-start_time:0.3f} seconds")


if __name__ == "__main__":
    logging.basicConfig()
    run()