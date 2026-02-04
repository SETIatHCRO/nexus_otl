from concurrent import futures
import logging
import time
from dataclasses import dataclass

import grpc
import metadata_pb2_grpc

from typing import Iterable, List
from math import pi
from os.path import join as join_as_path

from blri.fileformats import telinfo
from astropy.time import Time
import astropy.units as units
from astropy.utils import iers


from ata_snap import ata_rfsoc_fengine
import casperfpga

from ATATools import ata_control
from SNAPobs import snap_config
from SNAPobs.snap_hpguppi import snap_hpguppi_defaults as hpguppi_defaults

from metadata_pb2 import Metadata

@dataclass(frozen=True)
class AntLo:
    antenna_name: str
    lo_id: str

    def atatab_str(self) -> str:
        return f"{self.antenna_name.lower()}{self.lo_id.upper()}"


def _get_tuning_data(tunings: Iterable):
    return {
        tuning: ata_control.get_sky_freq(lo=tuning)
        for tuning in tunings
    }


def _get_antlo_mapped_fengines(antenna_names: List[str], tunings: List[str]):
    return {
        AntLo(i.ANT_name, i.LO): ata_rfsoc_fengine.AtaRfsocFengine(
            i.snap_hostname,
            pipeline_id=int(i.snap_hostname[-1])-1
        )
        for i in snap_config.get_ata_snap_tab().itertuples()
        if i.ANT_name in antenna_names and i.LO in tunings
    }

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
   

def read_chan_dest_ips(feng) -> List[ChannelSubbandDestinationDetail]:
    '''
    Processes the packet-details of an interface in the FEngines object,
    returning the destination IP addresses of the FEngine's channels.
    The output is collapsed on matching destination IP addresses, resulting
    in a list of IP addresses per range of channels.

    Parameters
    ----------
    feng: ata_snap_fengine
    The FEngine in question
    interface: int
    The interface enumeration of the FEngine in questions

    Returns
    -------
    [{dest: ip_str, start: int, end: int, header: dict} ... ]
    '''
    interface_map_enabled = {}
    for interface in range(feng.n_interfaces):
        try:
            interface_map_enabled[interface] = (feng.fpga.read_uint("eth%d_ctrl" % interface) & 0x00000002) != 0
        except casperfpga.transport_katcp.KatcpRequestFail:
            print('Failed to query ethernet status of {}[{}]'.format(feng.host, interface))
            return None

    if not any(interface_map_enabled.values()):
        print('The ethernet outputs of {} are all disabled: {}'.format(feng.host, interface_map_enabled))
        return []

    channel_subbands = []
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
            print('Failed to query headers of {}[{}]'.format(feng.host, interface))
            return None

        # Merge subsequent packet details into stream details
        strm_details = []
        for header in feng_headers:
            if header['valid'] and header['first']:
                ip = header['dest']
                
                if len(strm_details) > 0 and strm_details[-1].destination_ip == ip:
                    strm_details[-1].end_chan = header['chans']
                else:
                    strm_details.append(
                        FEnginePacketDestinationDetail(
                            ip,
                            header['chans'],
                            header['chans'],
                            header['n_chans'],
                            header['is_8_bit']
                        )
                    )

        # finalise streams as channel-subbands
        for strm_detail in strm_details:
            strm_end_chan = strm_detail.end_chan + strm_detail.packet_nchan
            strm_nchans = strm_end_chan-strm_detail.start_chan
            n_strm = strm_nchans / strm_detail.packet_nchan
            
            if strm_nchans % strm_detail.packet_nchan != 0:
                raise RuntimeError(
                'Read headers from {} that indicate non-integer number of streams, there is probably an issue in the collation procedure: {} / {} = {}'.format(
                    feng.host, strm_nchans, strm_detail.packet_nchan, n_strm
                ))

            channel_subbands.append(ChannelSubbandDestinationDetail(
                destination_ip=strm_detail.destination_ip,
                start=strm_detail.start_chan,
                stop=strm_end_chan,
                nof_packet_streams=int(n_strm)
            ))

    return channel_subbands


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

    def _get_antenna_tuning_band_info(antenna_names: List[str], tunings: list[str]):
        antlo_map_fengine = _get_antlo_mapped_fengines(antenna_names, tunings)

        antlo_map_chanband_details = {
            antlo: read_chan_dest_ips(feng)
            for antlo, feng in antlo_map_fengine.items()
        }
        tuning_map_freq = _get_tuning_data(tunings)

        md_dict = {}
        for antlo, chanband_details in antlo_map_chanband_details.items():
            base_path = join_as_path(
                "/v1/observatory/antenna",
                antlo.antenna_name,
                "tunings",
                antlo.lo_id,
                "bands"
            )
            fengine_n_chans = antlo_map_fengine[antlo].n_chans_f
            fengine_n_bits = 8
            fengine_meta_keyvalues = hpguppi_defaults.fengine_meta_key_values(fengine_n_bits, fengine_n_chans)
            fengine_center_chan = fengine_meta_keyvalues['FENCHAN']/2
            fengine_chan_bw = fengine_meta_keyvalues['FOFF']

            lo_obsfreq = tuning_map_freq[antlo.lo_id]
            chanband_details.sort(key = lambda csdd: csdd.start)

            md_dict[join_as_path(base_path, "len")] = len(chanband_details)
            for band_idx, chanband in enumerate(chanband_details):
                chan_path = join_as_path(base_path, str(band_idx))
                md_dict[join_as_path(chan_path, "channel_start")] = chanband.start
                md_dict[join_as_path(chan_path, "channel_stop")] = chanband.stop
                md_dict[join_as_path(chan_path, "address")] = chanband.destination_ip

                chanband_width = chanband.stop-chanband.start
                band_center_chan = chanband_width/2 + chanband.start + 0.5 # np.mean(np.array(chan_lst) + 0.5)
                band_center_freq = lo_obsfreq + (band_center_chan - fengine_center_chan - 0.5)*fengine_chan_bw

                md_dict[join_as_path(chan_path, "frequency_start")] = band_center_freq - fengine_chan_bw*chanband_width/2
                md_dict[join_as_path(chan_path, "frequency_stop")] = band_center_freq + fengine_chan_bw*chanband_width/2

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
        md_dict.update(MetadataServer._get_antenna_tuning_band_info(
            [ant.name for ant in t.antennas],
            tunings="abcd"
        ))
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
        start_time = time.time()
        if len(request.keys) == 0:
            md_dict.update(MetadataServer._get())
        for key in request.keys:
            if key == "/v1/observatory":
                md_dict.update(MetadataServer._get())
            elif key == "/v1/observation/iers":
                md_dict.update(MetadataServer._get_iers())
            else:
                md_dict.update({
                    "/v1/error/": f"Unexpected key '{key}'"
                })
        elapsed_time = time.time() - start_time
        print(f"Handled request keys {request.keys} in {elapsed_time:0.3f} s, returning {len(md_dict)} key-values.")
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