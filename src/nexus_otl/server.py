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
import struct
import base64
import threading
import socket

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from blri.fileformats import telinfo
from astropy.time import Time
import astropy.units as units
from astropy.utils import iers

from ata_snap import ata_rfsoc_fengine
import casperfpga

from ATATools.ata_rest import ATARest
# ATARest._debug = True
from ATATools import ata_control
from SNAPobs import snap_config
from SNAPobs.snap_hpguppi import snap_hpguppi_defaults as hpguppi_defaults

log = logging.getLogger("otl-server")
logging.getLogger("ATATools.ata_control").setLevel(logging.WARNING)

# ── Data classes ──────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class AntLo:
    antenna_name: str
    lo_id: str

    def atatab_str(self) -> str:
        return f"{self.antenna_name.lower()}{self.lo_id.upper()}"


@dataclass
class ChannelSubbandDestinationDetail:
    source_ip: str
    source_port: int
    destination_ip: str
    start: int
    stop: int
    nof_packet_streams: int
    nof_bits: int


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


def _get_antlo_mapped_fengines_generator(antenna_names: list[str], tunings: list[str]):
    for i in snap_config.get_ata_snap_tab().itertuples():
        if i.ANT_name in antenna_names and i.LO in tunings:
            yield (
                AntLo(i.ANT_name, i.LO),
                ata_rfsoc_fengine.AtaRfsocFengine(
                    i.snap_hostname,
                    pipeline_id=int(i.snap_hostname[-1]) - 1,
                )
            )


def read_chan_dest_ips(feng) -> list[ChannelSubbandDestinationDetail] | None:
    interface_map_enabled = {}
    gbe_keys = list(feng.fpga.gbes.keys())
    gbe_keys.sort()
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

        try:
            gbe = feng.fpga.gbes[gbe_keys[feng.output_id]]
        except IndexError:
            log.warning(f"FEngine {feng.host} has output_id ({feng.output_id}) that is out of range ({len(gbe_keys)}). Falling back to interface {interface}.")
            gbe = feng.fpga.gbes[gbe_keys[interface]]

        src_ip, src_port = str(gbe.ip_address), gbe.port
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
                    source_ip=src_ip,
                    source_port=src_port,
                    destination_ip=strm_detail.destination_ip,
                    start=strm_detail.start_chan,
                    stop=strm_end_chan,
                    nof_packet_streams=int(n_strm),
                    nof_bits=8 if strm_detail.is_8bit else 4,
                )
            )

    return channel_subbands


def _disconnect_fengines(antlo_map_fengine: dict) -> None:
    """Disconnect all FPGA transports to avoid leaking file descriptors."""
    for antlo, feng in antlo_map_fengine.items():
        feng.fpga.set_log_level("CRITICAL")
        feng.fpga.disconnect()


@dataclass
class FengineConnector:
    _connections: dict[AntLo, ata_rfsoc_fengine.AtaRfsocFengine]
    _lock: threading.Lock

    def __init__(self):
        self._connections = {}
        self._lock = threading.Lock()

    def __enter__(self):
        self._lock.acquire()
    
    def __exit__(self, exc_type, exc_value, traceback):
        self._lock.release()

    def __getitem__(self, key: AntLo) -> ata_rfsoc_fengine.AtaRfsocFengine:
        assert self._lock.locked, f"Use the context manager block!"
        if isinstance(key, tuple):
            key = AntLo(*key)
        assert isinstance(key, AntLo)
        if key not in self._connections:
            # TODO better indexing of tuples...
            for i in snap_config.get_ata_snap_tab().itertuples():
                if i.ANT_name == key.antenna_name and i.LO == key.lo_id:
                    self._connections[key] = ata_rfsoc_fengine.AtaRfsocFengine(
                        i.snap_hostname,
                        pipeline_id=int(i.snap_hostname[-1]) - 1,
                    )
                    self._connections[key].fpga.get_system_information()
                    break
        # TODO manage reconnecting to the FEngine if programming has occurred
        return self._connections[key]

    def items(self):
        with self:
            for item in self._connections.items():
                yield item
        

# ── Metadata collection ───────────────────────────────────────────────────────
def _safe_ata_control_get(func, antenna_names):
    ret = {}
    for ant_name in antenna_names:
        try:
            ret.update(
                func([ant_name])
            )
        except:
            ret.update({ant_name: None})
    return ret

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

        if ant_map_source[ant_detail.name] is not None:
            md[join_as_path(base, "pointing", "source_name")] = ant_map_source[ant_detail.name]
        if ant_map_radec[ant_detail.name] is not None:
            md[join_as_path(base, "pointing", "ra")] = ant_map_radec[ant_detail.name][0] * pi / 12
            md[join_as_path(base, "pointing", "dec")] = ant_map_radec[ant_detail.name][1] * pi / 180
        if ant_map_azel[ant_detail.name] is not None:
            md[join_as_path(base, "pointing", "az")] = ant_map_azel[ant_detail.name][0] * pi / 180
            md[join_as_path(base, "pointing", "el")] = ant_map_azel[ant_detail.name][1] * pi / 180

    return md

def _get_antenna_tuning_weights(tuning: str, antenna_name_map_channel_range: dict[str, tuple[int,int]]):
    with open(f"/opt/mnt/share/weights_{tuning}.bin", "rb") as fio:
        int_bytes = fio.read(struct.calcsize("<i"))
        fio_nants = struct.unpack("<i", int_bytes)[0]

        int_bytes = fio.read(struct.calcsize("<i"))
        fio_nchan = struct.unpack("<i", int_bytes)[0]

        int_bytes = fio.read(struct.calcsize("<i"))
        fio_npol = struct.unpack("<i", int_bytes)[0]

        ant_name_string = ""
        ant_names = []
        while len(ant_names) < fio_nants:
            ant_char = fio.read(1).decode("ascii")
            if ant_char == "\0":
                ant_names.append(ant_name_string)
                ant_name_string = ""
                continue

            assert ant_char.isalnum(), f"Unexpected character for antenna name: '{ant_char}' at byte {fio.tell()}"
            ant_name_string += ant_char

        fio_weights_offset = fio.tell()
        fio_weight_size = struct.calcsize("<2d")

        antenna_weights = {}
        for ant_name, channel_range in antenna_name_map_channel_range.items():
            ant_index = ant_names.index(ant_name)
            assert channel_range[0] >= 0 and channel_range[0] < fio_nchan, f"Channel range for '{ant_name}' lower bound is out of range [0, {fio_nchan}]: {channel_range[0]}"
            assert channel_range[1] > channel_range[0] and channel_range[1] < fio_nchan, f"Channel range for '{ant_name}' upper bound is out of range [{channel_range[0]}, {fio_nchan}]: {channel_range[1]}"

            fio.seek(
                fio_weights_offset
                + ant_index*fio_nchan*fio_npol*fio_weight_size
                + channel_range[0]*fio_npol*fio_weight_size
            )
            antenna_weights[ant_name] = [
                complex(*struct.unpack(
                    '<2d',
                    fio.read(fio_weight_size)
                ))
                for _ in range(channel_range[1]-channel_range[0])
            ]
        return antenna_weights


def _get_antenna_tuning_band_weight_info(antenna_name_map_tuning_band_channel_range: dict[str, list[tuple[int,int]]], tuning: str):
    ant_map_weights = _get_antenna_tuning_weights(
        tuning,
        {
            ant_name: (
                0,
                max(cr[1] for cr in band_chanrange),
            )
            for ant_name, band_chanrange in antenna_name_map_tuning_band_channel_range.items()
        }
    )

    md: dict[str, Any] = {}
    for ant_name, ant_weights in ant_map_weights.items():
        base = join_as_path(
            "/v1/observatory/antenna",
            ant_name,
            "tunings",
            tuning,
            "bands",
        )
        antband_chanranges = antenna_name_map_tuning_band_channel_range[ant_name]
        md[join_as_path(base, "len")] = len(antband_chanranges)
        for band_idx, band_chanrange in enumerate(antband_chanranges):
            md[join_as_path(base, str(band_idx), "weights")] = base64.b64encode(
                b''.join(struct.pack("<dd", w.real, w.imag) for w in ant_weights[band_chanrange[0]:band_chanrange[1]])
            )
    
    return md


def _get_antenna_fengine_info(
    antlo: AntLo,
    feng: ata_rfsoc_fengine.AtaRfsocFEngine
) -> dict[str, Any]:
    md: dict[str, Any] = {}
    base = join_as_path(
        "/v1/observatory/antenna",
        antlo.antenna_name,
        "tunings",
        antlo.lo_id,
        "fengine"
    )
    try:
        md[join_as_path(base, "synctime")] = feng.fpga.read_int('sync_sync_time')
        fengine_n_chans = feng.n_chans_f
        fengine_n_bits = 8 # should read from packet_details
        fengine_meta = hpguppi_defaults.fengine_meta_key_values(fengine_n_bits, fengine_n_chans)
        md[join_as_path(base, "nof_channels")] = fengine_n_chans
        md[join_as_path(base, "channel_bandwidth")] = fengine_meta["FOFF"]
        md[join_as_path(base, "sample_period")] = fengine_meta['TBIN']

        md[join_as_path(base, "hostname")] = feng.host
        md[join_as_path(base, "ip_address")] = socket.gethostbyname(feng.host)
        md[join_as_path(base, "pipeline_id")] = feng.pipeline_id
    except:
        pass
    
    return md

def _get_antenna_tuning_band_info(
    antlo: AntLo,
    feng: ata_rfsoc_fengine.AtaRfsocFEngine,
    tuning_map_freq: dict[str, float]
) -> dict[str, Any]:
    chanband_details = read_chan_dest_ips(feng)

    md: dict[str, Any] = {}
    if chanband_details is None:
        return md
    base = join_as_path(
        "/v1/observatory/antenna",
        antlo.antenna_name,
        "tunings",
        antlo.lo_id,
        "bands",
    )
    fengine_n_bits = set(d.nof_bits for d in chanband_details)
    assert len(fengine_n_bits) == 1, f"Mulitple bit widths in channels of F-Engine ({antlo}): {fengine_n_bits}"
    fengine_n_bits = fengine_n_bits.pop()
    fengine_meta = hpguppi_defaults.fengine_meta_key_values(fengine_n_bits, feng.n_chans_f)
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
        md[join_as_path(chan_path, "source_address")] = chanband.source_ip
        md[join_as_path(chan_path, "source_port")] = chanband.source_port

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


def get_observatory(
    fengine_connector: FengineConnector
) -> dict[str, Any]:
    t = telinfo.load_telescope_metadata("/opt/mnt/share/telinfo_ata.toml")
    md: dict[str, Any] = {}
    md[join_as_path("/v1/observatory", "time")] = time.time()
    md[join_as_path("/v1/observatory", "name")] = t.telescope_name
    md[join_as_path("/v1/observatory", "coordinates", "longitude")] = t.longitude
    md[join_as_path("/v1/observatory", "coordinates", "latitude")] = t.latitude
    md[join_as_path("/v1/observatory", "coordinates", "altitude")] = t.altitude
    md.update(_get_antenna_info(t.antennas))
    
    tunings = "abcd"
    tuning_map_freq = _get_tuning_data(tunings)
    with fengine_connector:
        for antlo in [
            AntLo(ant.name, tuning)
            for ant in t.antennas
            for tuning in tunings
        ]:
            try:
                feng = fengine_connector[antlo]
            except casperfpga.transport_katcp.KatcpRequestFail:
                log.warning(f"Katcp Request Failed for {antlo}")
                continue
            except KeyError:
                continue

            md.update(
                _get_antenna_tuning_band_info(
                    antlo,
                    feng,
                    tuning_map_freq
                )
            )
            md.update(
                _get_antenna_fengine_info(
                    antlo,
                    feng
                )
            )

    for tuning in tunings:
        md.update(_get_antenna_tuning_band_weight_info(
            {
                ant.name: [
                    (
                        md[join_as_path(
                            "/v1/observatory/antenna",
                            ant.name,
                            "tunings",
                            tuning,
                            "bands",
                            str(band_idx),
                            "channel_start"
                        )],
                        md[join_as_path(
                            "/v1/observatory/antenna",
                            ant.name,
                            "tunings",
                            tuning,
                            "bands",
                            str(band_idx),
                            "channel_stop"
                        )]
                    )
                    for band_idx in range(int(md[
                        join_as_path(
                            "/v1/observatory/antenna",
                            ant.name,
                            "tunings",
                            tuning,
                            "bands",
                            "len"
                        )
                    ]))
                ]
                for ant in t.antennas
                if join_as_path(
                    "/v1/observatory/antenna",
                    ant.name,
                    "tunings",
                    tuning,
                    "bands",
                    "len"
                ) in md
            },
            tuning
        ))

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
    iers_table.close()
    return md


# ── Value serialization ───────────────────────────────────────────────────────
def _value_type(v: Any) -> str:
    """Return the type tag for a Python value."""
    if isinstance(v, str):
        return "string"
    elif isinstance(v, bytes):
        return "vec<b64>"
    elif isinstance(v, bool):
        return "u32"
    elif isinstance(v, int):
        return "i64" if v < 0 else "u64"
    else:
        return "f64"


def _collect_metadata(keys: list[str], fengine_connector: FengineConnector) -> dict[str, Any]:
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
        md_dict.update(get_observatory(fengine_connector))
    for key in keys:
        if key == "/v1/observatory":
            md_dict.update(get_observatory(fengine_connector))
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

fengine_connector = FengineConnector()

class MetadataRequest(BaseModel):
    keys: list[str] = []


@app.post("/api/v1/otl/metadata")
async def query_metadata(body: MetadataRequest) -> dict:
    global fengine_connector
    return _collect_metadata(body.keys, fengine_connector)


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

