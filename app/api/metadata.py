import json
import logging
from typing import TypeVar

import geopandas as gpd
import pandas as pd
import pytz
import redis
from dask import dataframe
from dateutil import parser
from fastapi import APIRouter, Depends, HTTPException

from ..core.config import (BASE_URL, CURRENT_API_VERSION, M2M_URL, REDIS_HOST,
                           REDIS_PORT)
from ..store import META
from ..utils.conn import send_request
from ..utils.parsers import parse_annotations_json, unix_time_millis
from .cables import RSN_CABLE

router = APIRouter()

logging.root.setLevel(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    redis_cache = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), db=0)
except Exception as e:
    logger.error(e)

Choosable = TypeVar("Choosable", str, pd.DataFrame)


async def _check_version(version: float = 2.0):
    if version == CURRENT_API_VERSION:
        return True
    else:
        raise HTTPException(status_code=404, detail="API version not found.")


async def _prepare_anno_request(ref: str, start_dt: str, end_dt: str):
    rd_list = ref.split(",")
    begin_date = unix_time_millis(parser.parse(start_dt).replace(tzinfo=pytz.UTC))
    end_date = unix_time_millis(parser.parse(end_dt).replace(tzinfo=pytz.UTC))
    return {'rd_list': rd_list, 'begin_date': begin_date, 'end_date': end_date}


def _df_to_record(df: pd.DataFrame) -> str:
    return json.loads(df.to_json(orient="records"))


def _df_to_gdf_points(df: pd.DataFrame) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        df, crs={"init": "epsg:4326"}, geometry=gpd.points_from_xy(df["lon"], df["lat"]),
    )


async def _fetch_table(table_name: str, record: bool = False) -> Choosable:
    tabledf = META['dfdict'][table_name]
    if record:
        tabledf = _df_to_record(tabledf)
    return tabledf


async def _get_annotations(reference_designator, stream_method, stream_rd, begin_date, end_date):
    url = f"{BASE_URL}/{M2M_URL}/12580/anno/find"
    params = {
        "beginDT": begin_date,
        "endDT": end_date,
        "method": stream_method,
        "refdes": reference_designator,
        "stream": stream_rd,
    }
    anno_dct = send_request(url, params=params)
    if anno_dct:
        if "status_code" in anno_dct:
            raise HTTPException(
                status_code=anno_dct['status_code'],
                detail=f"OOINet Message: {anno_dct['message']}",
            )
        annodf = parse_annotations_json(anno_dct)
        return annodf


def _get_data_availability(refdes):
    icdf = pd.DataFrame(META['catalog_list'])
    inst_list = icdf[icdf.instrument_rd.str.match(refdes)]
    res = {}
    inst = None
    if len(inst_list) == 1:
        inst = inst_list.iloc[0]
    else:
        for idx, row in inst_list.iterrows():
            if (row["stream_rd"] == row["instrument"]["preferred_stream"]) and (
                row["stream_method"] == row["instrument"]["preferred_stream_method"]
            ):
                inst = row

    if not isinstance(inst, type(None)):
        dest_fold = f"ooi-data/data_availability/{inst.data_table}"
        dadf = dataframe.read_parquet(f"s3://{dest_fold}", index=False).compute()
        for idx, val in dadf.iterrows():
            res[str(val["dtindex"].astype("int64"))] = int(val["count"].astype("int64"))
    return res


@router.get("/arrays")
async def get_arrays(version: bool = Depends(_check_version)):
    if version:
        results = await _fetch_table("arrays", record=True)
    return results


@router.get("/infrastructures")
async def get_infrastructures(version: bool = Depends(_check_version)):
    if version:
        results = await _fetch_table("infrastructures", record=True)
    return results


@router.get("/instruments")
async def get_instruments(version: bool = Depends(_check_version)):
    if version:
        results = await _fetch_table("instruments", record=True)
    return results


@router.get("/parameters")
async def get_parameters(version: bool = Depends(_check_version)):
    if version:
        results = await _fetch_table("parameters", record=True)
    return results


@router.get("/streams")
async def get_streams(version: bool = Depends(_check_version)):
    if version:
        results = await _fetch_table("streams", record=True)
    return results


@router.get("/sites")
async def get_sites(version: bool = Depends(_check_version), geojson: bool = True):
    if version:
        tabledf = await _fetch_table("sites")
        tabledf = tabledf[tabledf.active_display == True]  # noqa
        if geojson:
            results = json.loads(_df_to_gdf_points(tabledf).to_json())
        else:
            results = _df_to_record(tabledf)
    return results


@router.get("/get_instruments_catalog")
async def get_instruments_catalog(version: bool = Depends(_check_version)):
    if version:
        results = META['catalog_list']
    return results


@router.get("/global_ranges")
async def get_global_ranges(version: bool = Depends(_check_version)):
    if version:
        global_ranges = json.loads(META['global_ranges'].to_json(orient='records'))

    return global_ranges


@router.get("/cables")
async def get_cables(version: bool = Depends(_check_version)):
    if version:
        return RSN_CABLE


@router.get("/get_deployments")
def get_deployments(version: bool = Depends(_check_version), refdes: str = ""):
    deployments = list(
        filter(lambda dep: dep['reference_designator'] == refdes, META['deployments_list'],)
    )

    return deployments


@router.get("/data_availability")
def get_data_availability(ref: str, version: bool = Depends(_check_version)):
    data_availability = {}
    if version and ref:
        data_availability_res = _get_data_availability(ref)
        data_availability = {ref: data_availability_res}

    return data_availability


@router.get("/get_annotations")
async def get_annotations(
    prepped_request: dict = Depends(_prepare_anno_request), version: bool = Depends(_check_version),
):
    if version:
        annotations = {"annotations": {}, "count": 0}
        count = 0
        for rd in prepped_request['rd_list']:
            r = rd.split("-")
            refdes = "-".join(r[:4])
            stream_method = r[-2]
            stream_rd = r[-1]
            anno_df = await _get_annotations(
                reference_designator=refdes,
                stream_method=stream_method,
                stream_rd=stream_rd,
                begin_date=prepped_request['begin_date'],
                end_date=prepped_request['end_date'],
            )
            anno = []
            if isinstance(anno_df, pd.DataFrame):
                anno = json.loads(anno_df.to_json(orient="records"))
            count += len(anno)

            annotations["annotations"].update({refdes: anno})
        annotations["count"] = count
        return annotations


@router.get("/status")
async def get_service_status():
    return {'status': 'running', 'message': 'Metadata service is up.'}
