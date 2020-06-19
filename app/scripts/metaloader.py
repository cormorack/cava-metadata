import datetime
import itertools as it
import json
import logging
import os
import pickle

import gspread
import pandas as pd

from ..core.config import BASE_PATH, BASE_URL, GOOGLE_SERVICE_JSON, M2M_URL
from ..store import META
from ..utils.conn import map_concurrency, send_request
from .baseloader import Loader

logging.root.setLevel(level=logging.INFO)

logger = logging.getLogger(__name__)


def get_stream(stream):
    url = f"{BASE_URL}/{M2M_URL}/12575/stream/byname/{stream}"
    stream_dict = send_request(url)
    return {
        "stream_id": stream_dict["id"],
        "stream_rd": stream,
        "stream_type": stream_dict["stream_type"]["value"],
        "stream_content": stream_dict["stream_content"]["value"],
        "parameters": [p["name"] for p in stream_dict["parameters"]],
        "last_updated": datetime.datetime.utcnow().isoformat(),
    }


def fetch_streams(inst):
    logger.info(inst["reference_designator"])
    streams_list = []
    for stream in inst["streams"]:
        newst = stream.copy()
        newst["stream_rd"] = stream['stream']
        newst["stream_method"] = stream['method']
        newst.update(get_stream(stream["stream"]))
        del newst['method']
        del newst['stream']
        streams_list.append(
            dict(
                reference_designator=inst["reference_designator"],
                platform_code=inst["platform_code"],
                mooring_code=inst["mooring_code"],
                instrument_code=inst["instrument_code"],
                **newst,
            )
        )
    return streams_list


def split_refdes(refdes):
    rd_list = refdes.split('-')
    return (rd_list[0], rd_list[1], '-'.join(rd_list[2:]))


def parse_global_range_dataframe(global_ranges):
    """ Cleans up the global ranges dataframe """
    global_df = global_ranges[global_ranges.columns[:-3]]
    global_df.columns = [
        "reference_designator",
        "parameter_id_r",
        "parameter_id_t",
        "global_range_min",
        "global_range_max",
        "data_level",
        "units",
    ]
    return global_df


def retrieve_deployments(refdes):
    dep_port = 12587
    reflist = list(split_refdes(refdes))
    base_url_list = [BASE_URL, M2M_URL, str(dep_port), 'events/deployment/inv']
    dep_list = send_request('/'.join(base_url_list + reflist))
    deployments = []
    if isinstance(dep_list, list):
        if len(dep_list) > 0:
            for d in dep_list:
                print('/'.join(base_url_list + reflist + [str(d)]))
                dep = send_request('/'.join(base_url_list + reflist + [str(d)]))
                if len(dep) > 0:
                    deployment = dep[0]
                    dep_dct = {
                        'reference_designator': deployment['referenceDesignator'],
                        'uid': deployment['sensor']['uid'],
                        'description': deployment['sensor']['description'],
                        'owner': deployment['sensor']['owner'],
                        'manufacturer': deployment['sensor']['manufacturer'],
                        'deployment_number': deployment['deploymentNumber'],
                        'deployment_start': deployment['eventStartTime'],
                        'deployment_end': deployment['eventStopTime'],
                        'lat': deployment['location']['latitude'],
                        'lon': deployment['location']['longitude'],
                    }
                    deployments.append(dep_dct)
    return deployments


def read_cava_assets(asset_xfile='CAVA_Assets.xlsx'):
    xl = pd.ExcelFile(asset_xfile)
    dfdict = {}
    for name in xl.sheet_names:
        lower_name = name.lower()
        dfdict[lower_name] = xl.parse(name)
    return dfdict


def get_toc():
    url = f"{BASE_URL}/{M2M_URL}/12576/sensor/inv/toc"
    return send_request(url)


def get_vocab():
    url = f"{BASE_URL}/{M2M_URL}/12586/vocab"
    return send_request(url)


def get_global_ranges():
    url = 'https://raw.githubusercontent.com/ooi-integration/qc-lookup/master/data_qc_global_range_values.csv'
    return parse_global_range_dataframe(pd.read_csv(url))


def get_cava_instruments(dfdict):
    cava_instruments = dfdict["instruments"].copy()
    cava_instruments = cava_instruments.dropna(
        subset=["preferred_stream_method", "preferred_stream", "preferred_parameters",]
    )
    cava_instruments.loc[:, "data_table"] = cava_instruments.apply(
        lambda row: "-".join(
            [row.reference_designator, row.preferred_stream_method, row.preferred_stream,]
        ),
        axis=1,
    )

    return cava_instruments


def compile_instrument_streams(toc_dict):
    streams_list = map_concurrency(fetch_streams, toc_dict['instruments'], max_workers=30)
    streams_list = list(it.chain.from_iterable(streams_list))
    return streams_list


def compile_instrument_deployments(dfdict):
    inst_list = dfdict['instruments'].reference_designator.values
    dep_list = map_concurrency(retrieve_deployments, inst_list, max_workers=10)
    dep_list = list(it.chain.from_iterable(dep_list))
    return dep_list


def get_items(keys, orig_dict):
    new_dict = {}
    for k, v in orig_dict.items():
        if k in keys:
            new_dict[k] = v
    return new_dict


def rename_item(old_key, new_key, orig_dict):
    new_dict = orig_dict.copy()
    if old_key in new_dict:
        new_dict.update({new_key: new_dict[old_key]})
        del new_dict[old_key]
    return new_dict


def get_stream_only(stream):
    return rename_item(
        'stream_id',
        'm2m_id',
        rename_item(
            'stream_rd',
            'reference_designator',
            get_items(
                ['stream_rd', 'stream_method', 'stream_id', 'stream_type', 'stream_content',],
                stream,
            ),
        ),
    )


def get_infrastructure(infra_rd, dfdict):
    infra_df = dfdict['infrastructures'][
        dfdict['infrastructures'].reference_designator.str.match(infra_rd)
    ]
    if len(infra_df) == 1:
        return json.loads(
            dfdict['infrastructures'][
                dfdict['infrastructures'].reference_designator.str.match(infra_rd)
            ]
            .iloc[0]
            .to_json()
        )


def get_instrument(instrument_rd, dfdict):
    return json.loads(
        dfdict['instruments'][dfdict['instruments'].reference_designator.str.match(instrument_rd)]
        .iloc[0]
        .to_json()
    )


def get_site(site_rd, dfdict):
    return json.loads(
        dfdict['sites'][dfdict['sites'].reference_designator.str.match(site_rd)].iloc[0].to_json()
    )


def get_parameters(parameters, dfdict):
    return json.loads(
        dfdict['parameters'][dfdict['parameters'].reference_designator.isin(parameters)].to_json(
            orient='records'
        )
    )


def create_catalog_item(stream, dfdict):
    item = {}
    item['data_table'] = "-".join(
        [stream['reference_designator'], stream['stream_method'], stream['stream_rd'],]
    )
    item['instrument_rd'] = stream['reference_designator']
    item['site_rd'] = stream['platform_code']
    item['infra_rd'] = stream['mooring_code']
    item['inst_rd'] = stream['instrument_code']
    item['stream_rd'] = stream['stream_rd']
    item['stream_method'] = stream['stream_method']
    item['stream_type'] = stream['stream_type']
    item['parameter_rd'] = ','.join(stream['parameters'])
    item['stream'] = get_stream_only(stream)
    item['infrastructure'] = get_infrastructure(
        '-'.join([stream['platform_code'], stream['mooring_code']]), dfdict
    )
    item['instrument'] = get_instrument(stream['reference_designator'], dfdict)
    item['site'] = get_site(stream['platform_code'], dfdict)
    item['parameters'] = get_parameters(stream['parameters'], dfdict)
    return item


def create_instruments_catalog(dfdict, streams_list):
    cava_instruments = get_cava_instruments(dfdict)
    cava_streams = list(
        filter(
            lambda st: "-".join([st['reference_designator'], st['stream_method'], st['stream_rd'],])
            in cava_instruments.data_table.values,
            streams_list,
        )
    )
    catalog_list = [create_catalog_item(stream, dfdict) for stream in cava_streams]
    return catalog_list


class LoadMeta(Loader):
    def __init__(self):
        Loader.__init__(self)
        self._name = "MetaLoader"
        self._gspread_dir = os.path.join(os.path.expanduser("~"), '.config', 'gspread')

        if not os.path.exists(self._gspread_dir):
            os.mkdir(self._gspread_dir)

        self._dfdict = {}
        self._daemon = False

        self.fetch_creds()
        self.start()

    def run(self):
        while self._in_progress:
            self._logger.info("Metadata loader started.")
            self.initialize_metadata()
            self._logger.info("Done loading metadata.")
            self._in_progress = False

    def create_metadata(self):
        metadata = {}
        # Read cava assets
        self.read_cava_assets()
        metadata['dfdict'] = self._dfdict

        # Get OOI Instruments table of contents
        toc_dict = get_toc()
        metadata['toc_dict'] = toc_dict

        # Get instruments streams from OOI
        streams_list = compile_instrument_streams(toc_dict)
        metadata['streams_list'] = streams_list

        # Get instruments catalog
        catalog_list = create_instruments_catalog(self._dfdict, streams_list)
        metadata['catalog_list'] = catalog_list

        # Get deployments catalog
        deployments_list = compile_instrument_deployments(self._dfdict)
        metadata['deployments_list'] = deployments_list

        # Get global ranges
        global_ranges = get_global_ranges()
        metadata['global_ranges'] = global_ranges

        # Record last run datetime
        metadata['last_updated'] = datetime.datetime.utcnow().isoformat()

        return metadata

    def _perform_refresh(self, metadata_cache):
        self._logger.info(f"Creating new metadata..")
        metadata = self.create_metadata()
        META.update(metadata)

        # Write to pickle
        with open(metadata_cache, 'wb') as fp:
            pickle.dump(metadata, fp, pickle.HIGHEST_PROTOCOL)

    def initialize_metadata(self):
        meta_path = os.path.join(BASE_PATH, 'core/meta')
        metadata_cache = os.path.join(meta_path, 'metadata.pkl')
        if os.path.exists(metadata_cache):
            with open(metadata_cache, 'rb') as f:
                META.update(pickle.load(f))

            self._logger.info(f"Metadata last updated: {META['last_updated']}.")
            if (datetime.datetime.utcnow() - pd.to_datetime(META['last_updated'])).days > 0:
                self._logger.info(f"Refreshing stale metadata...")
                os.unlink(metadata_cache)
                self._perform_refresh(metadata_cache)
        else:
            self._perform_refresh(metadata_cache)

    def read_cava_assets(self):
        gc = gspread.service_account()
        wks = gc.open('CAVA_Assets')
        for ws in wks.worksheets():
            name = ws.title
            if name in [
                'Arrays',
                'Areas',
                'Sites',
                'Infrastructures',
                'Instruments',
                'Streams',
                'Parameters',
            ]:
                lower_name = name.lower()
                self._dfdict[lower_name] = pd.DataFrame(ws.get_all_records())

    def fetch_creds(self):
        self._fs.get(
            GOOGLE_SERVICE_JSON, os.path.join(self._gspread_dir, 'service_account.json'),
        )
