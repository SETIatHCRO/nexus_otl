from concurrent import futures
import logging
import time

import grpc
import metadata_pb2_grpc

from typing import List
from math import pi
from os.path import join as join_as_path

from google.protobuf import json_format

from ATATools import ata_control
from blri.fileformats import telinfo
from astropy.time import Time
import astropy.units as units
from astropy.utils import iers

from metadata_pb2 import Metadata

class MetadataServer(metadata_pb2_grpc.OTLServicer):
    def _get_antenna_info(antennas: List[telinfo.AntennaDetail]):
        md_dict = {}

        antenna_names = [ad.name for ad in antennas]
        ant_map_source = ata_control.get_eph_source(antenna_names)
        ant_map_radec = ata_control.get_ra_dec(antenna_names)
        ant_map_azel  = ata_control.get_az_el(antenna_names)
        
        for ant_i, ant_detail in enumerate(antennas):
            base_path = join_as_path("/v1/observatory/antenna", ant_detail.name)
            md_dict[join_as_path(base_path, "number")] = ant_detail.number
            md_dict[join_as_path(base_path, "diameter")] = ant_detail.diameter
            for i, p in enumerate("xyz"):
                md_dict[join_as_path(base_path, "position", p)] = ant_detail.position[i]

            md_dict[join_as_path(base_path, "pointing", "source_name")] = ant_map_source[ant_detail.name]
            md_dict[join_as_path(base_path, "pointing", "ra")] = ant_map_radec[ant_detail.name][0] * pi/12
            md_dict[join_as_path(base_path, "pointing", "dec")] = ant_map_radec[ant_detail.name][1] * pi/180
            md_dict[join_as_path(base_path, "pointing", "az")] = ant_map_azel[ant_detail.name][0] * pi/180
            md_dict[join_as_path(base_path, "pointing", "el")] = ant_map_azel[ant_detail.name][1] * pi/180

        return md_dict

    def _get_tuning_info():
        md_dict = {}

        for tuning in "ABCD":
            md_dict[join_as_path("/v1/observatory/tuning", tuning, "frequency")] = ata_control.get_sky_freq(lo=tuning)

        return md_dict

    def _get():
        t = telinfo.load_telescope_metadata("/opt/mnt/share/telinfo_ata.toml")
        md_dict = {}
        md_dict[join_as_path("/v1/observatory", "time")] = time.time()
        md_dict[join_as_path("/v1/observatory", "name")] = t.telescope_name
        md_dict[join_as_path("/v1/observatory", "coordinates", "longitude")] = t.longitude
        md_dict[join_as_path("/v1/observatory", "coordinates", "latitude")] = t.latitude
        md_dict[join_as_path("/v1/observatory", "coordinates", "altitude")] = t.altitude
        md_dict.update(MetadataServer._get_antenna_info(t.antennas))
        md_dict.update(MetadataServer._get_tuning_info())
        return md_dict

    def _get_iers():
        today = Time.now()
        iers.conf.iers_degraded_accuracy = "warn"
        iers_table = iers.IERS.open()

        md_dict = {}
        pm_x, pm_y = iers_table.pm_xy(today.jd1, today.jd2)
        md_dict[join_as_path("/v1/observation/iers", "pm_x_arcsec")] = pm_x.to(units.arcsec).value
        md_dict[join_as_path("/v1/observation/iers", "pm_y_arcsec")] = pm_y.to(units.arcsec).value
        md_dict[join_as_path("/v1/observation/iers", "ut1_utc")] = iers_table.ut1_utc(today.jd1, today.jd2).to(units.s).value
        return md_dict

    def _construct_metadata(d: dict) -> Metadata:
        md = Metadata()
        for k, v in d.items():
            try:
                if isinstance(v, str):
                    md.data[k].string = v
                elif isinstance(v, bool):
                    md.data[k].boolean = v
                elif isinstance(v, int):
                    md.data[k].i64 = v
                else:
                    md.data[k].f64 = v
            except BaseException as err:
                raise ValueError(f"Value of {type(v)} at {k} is problematic: {v}") from err
        
        return md

    def get_metadata(self, request, context):
        md_dict = {}
        if len(request.keys) == 0:
            md_dict.update(MetadataServer._get())
        for key in request.keys:
            if key == "/v1/observatory":
                md_dict.update(MetadataServer._get())
            elif key.startswith("/v1/observation/iers"):
                md_dict.update(MetadataServer._get_iers())
            else:
                md_dict.update({
                    "/v1/error/": f"Unexpected key '{key}'"
                })
                break
        return MetadataServer._construct_metadata(md_dict)


def serve(port: str = "50051"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    metadata_pb2_grpc.add_OTLServicer_to_server(MetadataServer(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("MetadataServer started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()