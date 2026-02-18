"""OTL Metadata Server — REST port of the gRPC MetadataServer.

Queries live telescope metadata from the ATA control system, FPGA F-Engines,
and IERS tables. Serves it as a flat key-value dict over HTTP.

Requires (pip install into ataobs conda env):
    pip install fastapi uvicorn

Usage:
    python otl_server.py [--host 0.0.0.0] [--port 8001]
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from math import pi
from os.path import join as join_as_path
from typing import Any, Iterable

import asyncio
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from blri.fileformats import telinfo
from astropy.time import Time
import astropy.units as units
from astropy.utils import iers

from ata_snap import ata_rfsoc_fengine
import casperfpga

from ATATools import ata_control
from SNAPobs import snap_config
from SNAPobs.snap_hpguppi import snap_hpguppi_defaults as hpguppi_defaults

log = logging.getLogger("otl-server")

# ── Data classes ──────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class AntLo:
    antenna_name: str
    lo_id: str

    def atatab_str(self) -> str:
        return f"{self.antenna_name.lower()}{self.lo_id.upper()}"


@dataclass
class ChannelSubbandDestinationDetail:
    destination_ip: str
    start: int
    stop: int
    nof_packet_streams: int


@dataclass
class FEnginePacketDestinationDetail:
    destination_ip: str
    start_chan: int
    end_chan: int
    packet_nchan: int
    is_8bit: bool


# ── Helpers ───────────────────────────────────────────────────────────────────
def _get_tuning_data(tunings: Iterable):
    return {
        tuning: ata_control.get_sky_freq(lo=tuning)
        for tuning in tunings
    }


def _get_antlo_mapped_fengines(antenna_names: list[str], tunings: list[str]):
    return {
        AntLo(i.ANT_name, i.LO): ata_rfsoc_fengine.AtaRfsocFengine(
            i.snap_hostname,
            pipeline_id=int(i.snap_hostname[-1]) - 1,
        )
        for i in snap_config.get_ata_snap_tab().itertuples()
        if i.ANT_name in antenna_names and i.LO in tunings
    }


def read_chan_dest_ips(feng) -> list[ChannelSubbandDestinationDetail] | None:
    interface_map_enabled = {}
    for interface in range(feng.n_interfaces):
        try:
            interface_map_enabled[interface] = (
                feng.fpga.read_uint("eth%d_ctrl" % interface) & 0x00000002
            ) != 0
        except casperfpga.transport_katcp.KatcpRequestFail:
            log.warning("Failed to query ethernet status of %s[%d]", feng.host, interface)
            return None

    if not any(interface_map_enabled.values()):
        log.warning("Ethernet outputs of %s are all disabled: %s", feng.host, interface_map_enabled)
        return []

    channel_subbands: list[ChannelSubbandDestinationDetail] = []
    for interface, enabled in interface_map_enabled.items():
        if not enabled:
            continue
        try:
            if isinstance(feng, ata_rfsoc_fengine.AtaRfsocFengine):
                feng._read_parameters_from_fpga()
                feng._calc_output_ids()
                feng_headers = feng._read_headers()
            else:
                feng_headers = feng._read_headers(interface)
        except casperfpga.transport_katcp.KatcpRequestFail:
            log.warning("Failed to query headers of %s[%d]", feng.host, interface)
            return None

        strm_details: list[FEnginePacketDestinationDetail] = []
        for header in feng_headers:
            if header["valid"] and header["first"]:
                ip = header["dest"]
                if strm_details and strm_details[-1].destination_ip == ip:
                    strm_details[-1].end_chan = header["chans"]
                else:
                    strm_details.append(
                        FEnginePacketDestinationDetail(
                            ip,
                            header["chans"],
                            header["chans"],
                            header["n_chans"],
                            header["is_8_bit"],
                        )
                    )

        for strm_detail in strm_details:
            strm_end_chan = strm_detail.end_chan + strm_detail.packet_nchan
            strm_nchans = strm_end_chan - strm_detail.start_chan
            n_strm = strm_nchans / strm_detail.packet_nchan

            if strm_nchans % strm_detail.packet_nchan != 0:
                raise RuntimeError(
                    "Read headers from {} that indicate non-integer number of streams: "
                    "{} / {} = {}".format(feng.host, strm_nchans, strm_detail.packet_nchan, n_strm)
                )

            channel_subbands.append(
                ChannelSubbandDestinationDetail(
                    destination_ip=strm_detail.destination_ip,
                    start=strm_detail.start_chan,
                    stop=strm_end_chan,
                    nof_packet_streams=int(n_strm),
                )
            )

    return channel_subbands


def _disconnect_fengines(antlo_map_fengine: dict) -> None:
    """Disconnect all FPGA transports to avoid leaking file descriptors."""
    for antlo, feng in antlo_map_fengine.items():
        try:
            feng.fpga.set_log_level(logging.CRITICAL)
            feng.fpga.disconnect()
        except Exception:
            pass


# ── Metadata collection ───────────────────────────────────────────────────────
def _get_antenna_info(antennas: list[telinfo.AntennaDetail]) -> dict[str, Any]:
    md: dict[str, Any] = {}

    antenna_names = [ad.name for ad in antennas]
    ant_map_source = ata_control.get_eph_source(antenna_names)
    ant_map_radec = ata_control.get_ra_dec(antenna_names)
    ant_map_azel = ata_control.get_az_el(antenna_names)

    for ant_detail in antennas:
        base = join_as_path("/v1/observatory/antenna", ant_detail.name)
        md[join_as_path(base, "number")] = ant_detail.number
        md[join_as_path(base, "diameter")] = ant_detail.diameter
        for i, p in enumerate("xyz"):
            md[join_as_path(base, "position", p)] = ant_detail.position[i]

        md[join_as_path(base, "pointing", "source_name")] = ant_map_source[ant_detail.name]
        md[join_as_path(base, "pointing", "ra")] = ant_map_radec[ant_detail.name][0] * pi / 12
        md[join_as_path(base, "pointing", "dec")] = ant_map_radec[ant_detail.name][1] * pi / 180
        md[join_as_path(base, "pointing", "az")] = ant_map_azel[ant_detail.name][0] * pi / 180
        md[join_as_path(base, "pointing", "el")] = ant_map_azel[ant_detail.name][1] * pi / 180

    return md


def _get_antenna_fengine_info(
    antlo_map_fengine: dict[str, ata_rfsoc_fengine.AtaRfsocFEngine]
) -> dict[str, Any]:
    md: dict[str, Any] = {}
    for antlo, feng in antlo_map_fengine.items():
        base = join_as_path(
            "/v1/observatory/antenna",
            antlo.antenna_name,
            "fengine"
        )
        try:
            md[join_as_path(base, "synctime")] = feng.fpga.read_int('sync_sync_time')
        except:
            pass
    
    return md

def _get_antenna_tuning_band_info(
    antlo_map_fengine,
    tuning_map_freq: dict[str, float]
) -> dict[str, Any]:
    antlo_map_chanband_details = {
        antlo: read_chan_dest_ips(feng) for antlo, feng in antlo_map_fengine.items()
    }

    md: dict[str, Any] = {}
    for antlo, chanband_details in antlo_map_chanband_details.items():
        if chanband_details is None:
            continue
        base = join_as_path(
            "/v1/observatory/antenna",
            antlo.antenna_name,
            "tunings",
            antlo.lo_id,
            "bands",
        )
        fengine_n_chans = antlo_map_fengine[antlo].n_chans_f
        fengine_n_bits = 8
        fengine_meta = hpguppi_defaults.fengine_meta_key_values(fengine_n_bits, fengine_n_chans)
        fengine_center_chan = fengine_meta["FENCHAN"] / 2
        fengine_chan_bw = fengine_meta["FOFF"]

        lo_obsfreq = tuning_map_freq[antlo.lo_id]
        chanband_details.sort(key=lambda csdd: csdd.start)

        md[join_as_path(base, "len")] = len(chanband_details)
        for band_idx, chanband in enumerate(chanband_details):
            chan_path = join_as_path(base, str(band_idx))
            md[join_as_path(chan_path, "channel_start")] = chanband.start
            md[join_as_path(chan_path, "channel_stop")] = chanband.stop
            md[join_as_path(chan_path, "address")] = chanband.destination_ip

            chanband_width = chanband.stop - chanband.start
            band_center_chan = chanband_width / 2 + chanband.start + 0.5
            band_center_freq = (
                lo_obsfreq + (band_center_chan - fengine_center_chan - 0.5) * fengine_chan_bw
            )

            md[join_as_path(chan_path, "frequency_start")] = (
                band_center_freq - fengine_chan_bw * chanband_width / 2
            )
            md[join_as_path(chan_path, "frequency_stop")] = (
                band_center_freq + fengine_chan_bw * chanband_width / 2
            )

    return md


def get_observatory() -> dict[str, Any]:
    t = telinfo.load_telescope_metadata("/opt/mnt/share/telinfo_ata.toml")
    md: dict[str, Any] = {}
    md[join_as_path("/v1/observatory", "time")] = time.time()
    md[join_as_path("/v1/observatory", "name")] = t.telescope_name
    md[join_as_path("/v1/observatory", "coordinates", "longitude")] = t.longitude
    md[join_as_path("/v1/observatory", "coordinates", "latitude")] = t.latitude
    md[join_as_path("/v1/observatory", "coordinates", "altitude")] = t.altitude
    md.update(_get_antenna_info(t.antennas))
    
    tunings = "abcd"
    antlo_map_fengine = _get_antlo_mapped_fengines(
        [ant.name for ant in t.antennas],
        tunings=tunings
    )
    tuning_map_freq = _get_tuning_data(tunings)
    try:
        md.update(
            _get_antenna_tuning_band_info(
                antlo_map_fengine,
                tuning_map_freq
            )
        )
        md.update(
            _get_antenna_fengine_info(
                antlo_map_fengine
            )
        )
    finally:
        _disconnect_fengines(antlo_map_fengine)

    return md


def get_iers() -> dict[str, Any]:
    today = Time.now()
    iers.conf.iers_degraded_accuracy = "warn"
    iers_table = iers.IERS.open()

    md: dict[str, Any] = {}
    pm_x, pm_y = iers_table.pm_xy(today.jd1, today.jd2)
    md[join_as_path("/v1/observation/iers", "pm_x_arcsec")] = pm_x.to(units.arcsec).value
    md[join_as_path("/v1/observation/iers", "pm_y_arcsec")] = pm_y.to(units.arcsec).value
    md[join_as_path("/v1/observation/iers", "ut1_utc")] = (
        iers_table.ut1_utc(today.jd1, today.jd2).to(units.s).value
    )
    return md


# ── Value serialization ───────────────────────────────────────────────────────
def _value_type(v: Any) -> str:
    """Return the type tag for a Python value."""
    if isinstance(v, str):
        return "string"
    elif isinstance(v, bool):
        return "u32"
    elif isinstance(v, int):
        return "i64" if v < 0 else "u64"
    else:
        return "f64"


def _collect_metadata(keys: list[str]) -> dict[str, Any]:
    """Collect metadata for the given keys (or all if empty).

    Returns flat list format::

        {
            "version": "v1",
            "data": [
                {"key": "observatory.time", "value": ..., "type": "f64"},
                {"key": "observation.iers.pm_x_arcsec", "value": ..., "type": "f64"},
            ]
        }
    """
    md_dict: dict[str, Any] = {}
    start = time.time()

    if not keys:
        md_dict.update(get_observatory())
    for key in keys:
        if key == "/v1/observatory":
            md_dict.update(get_observatory())
        elif key == "/v1/observation/iers":
            md_dict.update(get_iers())
        else:
            log.warning("Unexpected metadata key: %s", key)

    elapsed = time.time() - start
    log.info("Handled keys %s in %.3f s, returning %d key-values.", keys, elapsed, len(md_dict))

    data: list[dict[str, Any]] = []
    for path, value in md_dict.items():
        parts = path.strip("/").split("/")
        if len(parts) < 3:
            continue
        # Skip version segment, dot-join group + rest
        key = ".".join(parts[1:])
        data.append({
            "key": key,
            "value": value,
            "type": _value_type(value),
        })

    return {"version": "v1", "data": data}


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="OTL Metadata Server")


class MetadataRequest(BaseModel):
    keys: list[str] = []


@app.post("/api/v1/otl/metadata")
async def query_metadata(body: MetadataRequest) -> dict:
    return _collect_metadata(body.keys)


@app.get("/api/v1/meta/ping")
async def ping() -> dict:
    return {}


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="OTL Metadata Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    log.info("OTL Metadata Server starting on %s:%d", args.host, args.port)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()

