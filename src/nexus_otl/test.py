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

    md = Metadata()
    for ant_name in antenna_names:
        md.data[join_as_path("pointing", "source", ant_name)].string = source_s[ant_name]
        md.data[join_as_path("pointing", "right_ascension_rad", ant_name)].f64 = radec_s[ant_name][0] * pi/12
        md.data[join_as_path("pointing", "declination_rad", ant_name)].f64 = radec_s[ant_name][1] * pi/180
        md.data[join_as_path("pointing", "azimuth_rad", ant_name)].f64 = azel_s[ant_name][0] * pi/180
        md.data[join_as_path("pointing", "declination_rad", ant_name)].f64 = azel_s[ant_name][1] * pi/180

    return md

def get_lo_frequency(lo: str):
    skyfreq = ata_control.get_sky_freq(lo=lo)
    md = Metadata()
    md.data[join_as_path("frequency_MHz", "local_oscillator", lo)].f64 = skyfreq
    
    return md

def get_telescope_info():
    t = telinfo.load_telescope_metadata("/opt/mnt/share/telinfo_ata.toml")
    
    md = Metadata()
    md.data[join_as_path("telescope_info", "name")].string = t.telescope_name
    md.data[join_as_path("telescope_info", "longitude")].f64 = t.longitude
    md.data[join_as_path("telescope_info", "latitude")].f64 = t.latitude
    md.data[join_as_path("telescope_info", "altitude")].f64 = t.altitude

    md.data[join_as_path("telescope_info", "antenna_position_frame")].string = t.antenna_position_frame.value
    assert t.antenna_position_frame.value == "xyz" # assuming this...
    for ant_detail in t.antennas:
        md.data[join_as_path("telescope_info", "antenna", "name")].string = ant_detail.name
        md.data[join_as_path("telescope_info", "antenna", "number")].i64 = ant_detail.number
        md.data[join_as_path("telescope_info", "antenna", "diameter")].f64 = ant_detail.diameter
        for i, p in enumerate("xyz"):
            md.data[join_as_path("telescope_info", "antenna", "position", p)].f64 = ant_detail.position[i]
        
    return md

def get_mcast_group(antenna: str, channel_0_of_subband: int):
    # relate the antenna
    pass

def get():
    t = telinfo.load_telescope_metadata("/opt/mnt/share/telinfo_ata.toml")
    md = Metadata()
    md.data[join_as_path("telescope", "name")].string = t.telescope_name
    md.data[join_as_path("telescope", "longitude")].f64 = t.longitude
    md.data[join_as_path("telescope", "latitude")].f64 = t.latitude
    md.data[join_as_path("telescope", "altitude")].f64 = t.altitude

    antenna_names = [ad.name for ad in t.antennas]
    ant_map_source = ata_control.get_eph_source(antenna_names)
    ant_map_radec = ata_control.get_ra_dec(antenna_names)
    ant_map_azel  = ata_control.get_az_el(antenna_names)
    
    for ant_detail in t.antennas:
        base_path = join_as_path("antenna", ant_detail.name)
        md.data[join_as_path(base_path, "diameter")].f64 = ant_detail.diameter
        for i, p in enumerate("xyz"):
            md.data[join_as_path(base_path, "position", p)].f64 = ant_detail.position[i]

        md.data[join_as_path(base_path, "pointing", "source")].string = ant_map_source[ant_detail.name]
        md.data[join_as_path(base_path, "pointing", "right_ascension_rad")].f64 = ant_map_radec[ant_detail.name][0] * pi/12
        md.data[join_as_path(base_path, "pointing", "declination_rad")].f64 = ant_map_radec[ant_detail.name][1] * pi/180
        md.data[join_as_path(base_path, "pointing", "azimuth_rad")].f64 = ant_map_azel[ant_detail.name][0] * pi/180
        md.data[join_as_path(base_path, "pointing", "declination_rad")].f64 = ant_map_azel[ant_detail.name][1] * pi/180
    

    for tuning in "ABCD":
        md.data[join_as_path("tuning", tuning, "frequency_MHz")].f64 = ata_control.get_sky_freq(lo=tuning)

    return md

def get_iers():
    today = Time.now()
    iers.conf.iers_degraded_accuracy = "warn"
    iers_table = iers.IERS.open()

    md = Metadata()
    pm_x, pm_y = iers_table.pm_xy(today.jd1, today.jd2)
    md.data[join_as_path("iers", "pm_x_arcsec")].f64 = pm_x.to(units.arcsec).value
    md.data[join_as_path("iers", "pm_y_arcsec")].f64 = pm_y.to(units.arcsec).value
    md.data[join_as_path("iers", "ut1_utc")].f64 = iers_table.ut1_utc(today.jd1, today.jd2).to(units.s).value
    return md

# print(json_format.MessageToJson(get_pointing(["1b", "1c"])))
# print(json_format.MessageToJson(get_lo_frequency('A')))
# print(json_format.MessageToJson(get_telescope_info()))
# print(json_format.MessageToJson(get()))
print(json_format.MessageToJson(get_iers()))