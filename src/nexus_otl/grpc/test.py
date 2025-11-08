from google.protobuf import json_format
from metadata_pb2 import Metadata, Metadatum

md = Metadata()
datum = Metadatum()
md.data["double"].f64 = 3.141
md.data["str"].string = "3.141/3"

print(json_format.MessageToJson(md))
