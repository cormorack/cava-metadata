import concurrent.futures
import datetime
import itertools as it
import json
import logging
import os
import pickle
import re
import sys

import dask.dataframe as dd
import numpy as np
import pandas as pd
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://ooinet.oceanobservatories.org"
M2M_URL = "api/m2m"
USERNAME = os.environ["OOI_USERNAME"]
TOKEN = os.environ["OOI_TOKEN"]
SESSION = requests.Session()
a = requests.adapters.HTTPAdapter(max_retries=1000, pool_connections=1000, pool_maxsize=1000)
SESSION.mount("https://", a)
DISCRETE_SAMPLES_MAP = {
    '20150704': {
        'url': 'https://alfresco.oceanobservatories.org/alfresco/d/d/workspace/SpacesStore/b5a9b9bd-3c40-4d0a-8e26-a3ad69e4d21f/Cabled-6_TN326_Discrete_Summary_2019-10-21_ver_1-00.csv',
        'cruise_id': 'TN-326',
        'alfresco_path': 'OOI > Cabled Array > Cruise Data > Cabled-06_TN-326_2015-7-04 > Ship Data > Water Sampling',
    },
    '20160711': {
        'url': 'https://alfresco.oceanobservatories.org/alfresco/d/d/workspace/SpacesStore/1453a369-d77b-478a-bf69-233344f20226/Cabled-7_SKQ201610S_Discrete_Summary_2019-10-21_ver_1-00.csv',
        'cruise_id': 'SKQW201610S',
        'alfresco_path': 'OOI > Cabled Array > Cruise Data > Cabled-07_SKQW201610S_2016-07-11 > Shipboard Data > Water Sampling',
    },
    '20170727': {
        'url': 'https://alfresco.oceanobservatories.org/alfresco/d/d/workspace/SpacesStore/cbb984b6-44fc-4cba-9342-48f318024507/Cabled-8_RR1713-RR1717_Discrete_Summary_2019-10-21_ver_1-00.csv',
        'cruise_id': 'RR1713-RR1717',
        'alfresco_path': 'OOI > Cabled Array > Cruise Data > Cabled-08_RR1713-RR1717_2017-07-27 > Shipboard Data > Water Sampling',
    },
    '20180723': {
        'url': 'https://alfresco.oceanobservatories.org/alfresco/d/d/workspace/SpacesStore/360b799e-62d6-496f-83f4-88c7b1f6d806/Cabled-9_RR1809-RR1812_Discrete_Summary_2019-12-06_ver_1-01.csv',
        'cruise_id': 'RR1809-RR1812',
        'alfresco_path': 'OOI > Cabled Array > Cruise Data > Cabled-09_RR1809-RR1812_2018-07-23 > Ship Data > Water Sampling',
    },
    '20190805': {
        'url': 'https://alfresco.oceanobservatories.org/alfresco/d/d/workspace/SpacesStore/1383afef-5642-4223-9df4-7b74017a8c55/Cabled-10_AT4212_Discrete_Summary_2019-12-19_ver_1-10.csv',
        'cruise_id': 'AT4212',
        'alfresco_path': 'OOI > Cabled Array > Cruise Data > Cabled-10_AT4212_2019-08-05 > Ship Data > Water Sampling',
    },
}


# UTILITY FUNCTIONS ==========================
def map_concurrency(func, iterator, args=(), max_workers=10):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(func, i, *args): i for i in iterator}
        for future in concurrent.futures.as_completed(future_to_url):
            data = future.result()
            results.append(data)
    return results


def fetch_url(prepped_request, session=None, timeout=120, stream=False, **kwargs):

    session = session or requests.Session()
    r = session.send(prepped_request, timeout=timeout, stream=stream, **kwargs)

    if r.status_code == 200:
        logger.info(f"URL fetch {prepped_request.url} successful.")
        return r
    elif r.status_code == 500:
        message = "Server is currently down."
        if "ooinet.oceanobservatories.org/api" in prepped_request.url:
            message = "UFrame M2M is currently down."
        logger.warning(message)
        return r
    else:
        message = f"Request {prepped_request.url} failed: {r.status_code}, {r.reason}"
        logger.warning(message)  # noqa
        return r


def send_request(url, params=None):
    """Send request to OOI. Username and Token already included."""
    try:
        prepped_request = requests.Request(
            "GET", url, params=params, auth=(USERNAME, TOKEN)
        ).prepare()
        r = fetch_url(prepped_request, session=SESSION)
        if isinstance(r, requests.Response):
            return r.json()
    except Exception as e:
        logger.warning(e)


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


def clean_ship_verification(svdf):
    cleaned = svdf.dropna(how='all')
    cols = cleaned.columns

    names = []
    display_name = []
    units = []
    for col in cols:
        match = re.search(r"((\w+\s?)+)(\[.*\])?", col)
        if match:
            matches = match.groups()
            name = matches[0].strip()
            unit = matches[-1]
            if unit:
                unit = unit.strip('[]')
            names.append(name.lower().replace(' ', '_'))
            display_name.append(name)
            units.append(unit)

    # for later, maybe save into separate table?
    # discrete_samples_labels = {
    #     'name': names,
    #     'display_name': display_name,
    #     'unit': units
    # }

    cleaned.columns = names
    all_cleaned = (
        cleaned.replace(-9999999.0, np.NaN).replace('-9999999', np.NaN).dropna(subset=['cruise'])
    )
    time_cols = all_cleaned.columns[all_cleaned.columns.str.contains('time')]
    for col in time_cols:
        all_cleaned.loc[:, col] = all_cleaned[col].apply(pd.to_datetime)

    return all_cleaned


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


def create_metadata(meta_path):
    metadata = {}
    # Read cava assets
    dfdict = read_cava_assets(asset_xfile=os.path.join(meta_path, 'CAVA_Assets.xlsx'))
    metadata['dfdict'] = dfdict

    # Get OOI Instruments table of contents
    toc_dict = get_toc()
    metadata['toc_dict'] = toc_dict

    # Get instruments streams from OOI
    streams_list = compile_instrument_streams(toc_dict)
    metadata['streams_list'] = streams_list

    # Get instruments catalog
    catalog_list = create_instruments_catalog(dfdict, streams_list)
    metadata['catalog_list'] = catalog_list

    # Get deployments catalog
    deployments_list = compile_instrument_deployments(dfdict)
    metadata['deployments_list'] = deployments_list

    # Get global ranges
    global_ranges = get_global_ranges()
    metadata['global_ranges'] = global_ranges

    # # Get Discrete dataframes
    # DISCRETE_DFS = {}
    # for k, v in DISCRETE_SAMPLES_MAP.items():
    #     DISCRETE_DFS[k] = clean_ship_verification(pd.read_csv(v['url']))

    # metadata['DISCRETE_DFS'] = DISCRETE_DFS

    # Record last run datetime
    metadata['last_updated'] = datetime.datetime.utcnow().isoformat()

    return metadata


def initialize_metadata(meta_path='.'):
    # Setup metadata
    metadata_cache = os.path.join(meta_path, 'metadata.pkl')
    if os.path.exists(metadata_cache):
        with open(metadata_cache, 'rb') as f:
            metadata = pickle.load(f)

        # if (
        #     datetime.datetime.utcnow()
        #     - pd.to_datetime(metadata['last_updated'])
        # ).days > 0:
        #     metadata = create_metadata(meta_path)
    else:
        metadata = create_metadata(meta_path)

    # Write to pickle
    with open(metadata_cache, 'wb') as fp:
        pickle.dump(metadata, fp, pickle.HIGHEST_PROTOCOL)

    return metadata


def init_metadata(app):
    meta_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'meta')
    app.logger.info("INITIALIZING METADATA...")
    initialize_metadata(meta_path)
    app.logger.info("METADATA INITIALIZED.")
