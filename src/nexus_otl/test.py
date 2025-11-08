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

def get_pointing(antenna_names: List[str]):
    def _safe_ata_control_get(antlist, get_func):
        ret = {}
        for ant in antlist:
            try:
                ret.update(get_func([ant]))
            except:
                ret.update({ant: None})
        return ret

    source_s = _safe_ata_control_get(antenna_names, ata_control.get_eph_source)
    radec_s = _safe_ata_control_get(antenna_names, ata_control.get_ra_dec)
    azel_s  = _safe_ata_control_get(antenna_names, ata_control.get_az_el)

    md_dict = {}
    for ant_name in antenna_names:
        md_dict[join_as_path("/pointing", ant_name, "source")] = source_s[ant_name]
        md_dict[join_as_path("/pointing", ant_name, "right_ascension_rad")] = radec_s[ant_name][0] * pi/12
        md_dict[join_as_path("/pointing", ant_name, "declination_rad")] = radec_s[ant_name][1] * pi/180
        md_dict[join_as_path("/pointing", ant_name, "azimuth_rad")] = azel_s[ant_name][0] * pi/180
        md_dict[join_as_path("/pointing", ant_name, "declination_rad")] = azel_s[ant_name][1] * pi/180

    return md_dict

def get_lo_frequency(lo: str):
    skyfreq = ata_control.get_sky_freq(lo=lo)
    md_dict = {}
    md_dict[join_as_path("/frequency_MHz", "local_oscillator", lo)] = skyfreq
    
    return md_dict

def get_telescope_info():
    t = telinfo.load_telescope_metadata("/opt/mnt/share/telinfo_ata.toml")
    
    md_dict = {}
    md_dict[join_as_path("/telescope_info", "name")] = t.telescope_name
    md_dict[join_as_path("/telescope_info", "longitude")] = t.longitude
    md_dict[join_as_path("/telescope_info", "latitude")] = t.latitude
    md_dict[join_as_path("/telescope_info", "altitude")] = t.altitude

    md_dict[join_as_path("/telescope_info", "antenna_position_frame")] = t.antenna_position_frame.value
    assert t.antenna_position_frame.value == "xyz" # assuming this...
    for ant_detail in t.antennas:
        md_dict[join_as_path("/telescope_info", "antenna", ant_detail.name, "number")] = ant_detail.number
        md_dict[join_as_path("/telescope_info", "antenna", ant_detail.name, "diameter")] = ant_detail.diameter
        for i, p in enumerate("xyz"):
            md_dict[join_as_path("/telescope_info", "antenna", ant_detail.name, "position", p)] = ant_detail.position[i]
        
    return md_dict

def get_mcast_group(antenna: str, channel_0_of_subband: int):
    # relate the antenna
    pass

def get():
    t = telinfo.load_telescope_metadata("/opt/mnt/share/telinfo_ata.toml")
    md_dict = {}
    md_dict[join_as_path("/telescope", "name")] = t.telescope_name
    md_dict[join_as_path("/telescope", "longitude")] = t.longitude
    md_dict[join_as_path("/telescope", "latitude")] = t.latitude
    md_dict[join_as_path("/telescope", "altitude")] = t.altitude

    antenna_names = [ad.name for ad in t.antennas]
    ant_map_source = ata_control.get_eph_source(antenna_names)
    ant_map_radec = ata_control.get_ra_dec(antenna_names)
    ant_map_azel  = ata_control.get_az_el(antenna_names)
    
    for ant_detail in t.antennas:
        base_path = join_as_path("/antenna", ant_detail.name)
        md_dict[join_as_path(base_path, "diameter")] = ant_detail.diameter
        for i, p in enumerate("xyz"):
            md_dict[join_as_path(base_path, "position", p)] = ant_detail.position[i]

        md_dict[join_as_path(base_path, "pointing", "source")] = ant_map_source[ant_detail.name]
        md_dict[join_as_path(base_path, "pointing", "right_ascension_rad")] = ant_map_radec[ant_detail.name][0] * pi/12
        md_dict[join_as_path(base_path, "pointing", "declination_rad")] = ant_map_radec[ant_detail.name][1] * pi/180
        md_dict[join_as_path(base_path, "pointing", "azimuth_rad")] = ant_map_azel[ant_detail.name][0] * pi/180
        md_dict[join_as_path(base_path, "pointing", "declination_rad")] = ant_map_azel[ant_detail.name][1] * pi/180
    

    for tuning in "ABCD":
        md_dict[join_as_path("/tuning", tuning, "frequency_MHz")] = ata_control.get_sky_freq(lo=tuning)

    return md_dict

def get_iers():
    today = Time.now()
    iers.conf.iers_degraded_accuracy = "warn"
    iers_table = iers.IERS.open()

    md_dict = {}
    pm_x, pm_y = iers_table.pm_xy(today.jd1, today.jd2)
    md_dict[join_as_path("/iers", "pm_x_arcsec")] = pm_x.to(units.arcsec).value
    md_dict[join_as_path("/iers", "pm_y_arcsec")] = pm_y.to(units.arcsec).value
    md_dict[join_as_path("/iers", "ut1_utc")] = iers_table.ut1_utc(today.jd1, today.jd2).to(units.s).value
    return md_dict

def construct_metadata(d: dict) -> Metadata:
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

# print(json_format.MessageToJson(get_pointing(["1b", "1c"])))
# print(json_format.MessageToJson(get_lo_frequency('A')))
# print(json_format.MessageToJson(get_telescope_info()))
print(json_format.MessageToJson(construct_metadata(get())))
print(json_format.MessageToJson(construct_metadata(get_iers())))