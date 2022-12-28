import json
import os
import logging
import statistics
import math
from typing import TypeVar, Union, Tuple, Literal, Optional, List, Dict, Any
import fsspec

import geopandas as gpd
import pandas as pd
import pytz
import redis.asyncio as aioredis
import requests
from dask import dataframe
from dateutil import parser
import datetime
from shapely.geometry import Polygon
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from core.config import (
    BASE_URL,
    CURRENT_API_VERSION,
    M2M_URL,
    METADATA_SOURCE,
    settings,
)
from cache.redis import redis_dependency, ConnectionError
from store import META
from utils.conn import send_request, retrieve_deployments
from utils.parsers import parse_annotations_json, unix_time_millis
from utils.hash import hash_dict
from api.cables import RSN_CABLE
from api.models import InstrumentRequest

router = APIRouter()

logging.root.setLevel(level=logging.INFO)
logger = logging.getLogger('uvicorn')

Choosable = TypeVar("Choosable", str, pd.DataFrame)


async def _get_cache(
    cache: Optional[Any] = None, key: Optional[str] = None
) -> Any:
    """
    Retrieves cache from cache system

    Parameters
    ----------
    cache : object, optional
        Caching object that connects to the cache system,
        currently supports aioredis
    key : str
        The cache key to retrieve result from

    Returns
    -------
    Cache results

    """
    if cache is not None:
        return await cache.get(key)


async def _set_cache(
    cache: Optional[Any] = None,
    key: Optional[str] = None,
    value: Optional[Any] = None,
    expires: datetime.timedelta = datetime.timedelta(hours=1),
):
    """
    Stores values into the cache system

    Parameters
    ----------
    cache : object, optional
        Caching object that connects to the cache system,
        currently supports aioredis
    key : str
        The cache key to store result
    value : any
        The values to be stored in cache (Will be stored as JSON)

    Returns
    -------
    None

    """
    if cache is not None:
        # Stores cache to redis
        await cache.set(
            key,
            json.dumps(value).encode('utf-8'),
            ex=int(expires.total_seconds()),
        )


async def _check_version(version: float = 2.0):
    if version == CURRENT_API_VERSION:
        return True
    else:
        raise HTTPException(status_code=404, detail="API version not found.")


async def _prepare_anno_request(ref: str, start_dt: str, end_dt: str):
    rd_list = ref.split(",")
    begin_date = unix_time_millis(
        parser.parse(start_dt).replace(tzinfo=pytz.UTC)
    )
    end_date = unix_time_millis(parser.parse(end_dt).replace(tzinfo=pytz.UTC))
    return {"rd_list": rd_list, "begin_date": begin_date, "end_date": end_date}


def _df_to_record(df: pd.DataFrame) -> str:
    return json.loads(df.to_json(orient="records"))


def _df_to_gdf_points(df: pd.DataFrame) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        df,
        crs={"init": "epsg:4326"},
        geometry=gpd.points_from_xy(df["lon"], df["lat"]),
    )


async def _fetch_table(
    table_name: str,
    record: bool = False,
    filters: Union[list, None] = None,
    cache: Optional[Any] = None,
) -> Choosable:
    kwargs = locals()
    kwargs.pop("cache")

    # Create key hash from inputs
    cache_key = hash_dict(kwargs)
    cached_result = await _get_cache(cache, cache_key)

    if cached_result is not None:
        cached_dict = json.loads(cached_result)
        tabledf = pd.DataFrame(cached_dict)
    else:
        fs_kwargs = {
            k: v
            for k, v in settings.FILE_SYSTEMS["aws_s3"].items()
            if k != 'protocol'
        }

        tabledf = dataframe.read_parquet(
            os.path.join(METADATA_SOURCE, table_name),
            engine="pyarrow-dataset",
            filters=filters,
            index=False,
            storage_options=fs_kwargs,
            ignore_metadata_file=True,
        ).compute()
    if record:
        tabledf = _df_to_record(tabledf)

    await _set_cache(
        cache,
        cache_key,
        value=_df_to_record(tabledf)
        if isinstance(tabledf, pd.DataFrame)
        else tabledf,
    )
    return tabledf


async def _get_annotations(
    reference_designator, stream_method, stream_rd, begin_date, end_date
):
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
                status_code=anno_dct["status_code"],
                detail=f"OOINet Message: {anno_dct['message']}",
            )
        annodf = parse_annotations_json(anno_dct)
        return annodf


def _get_poly(row):
    return Polygon(json.loads(row))


def _df2dict(df: Union[pd.DataFrame, dataframe.DataFrame]) -> dict:
    """Converts data availability dataframe to dictionary"""
    result_dict = {}
    for _, row in df.iterrows():
        if row['inst_rd'] not in result_dict:
            result_dict[row['inst_rd']] = {}
        stream_name = "-".join(row['data_stream'].split('-')[-2:])
        result_dict[row['inst_rd']][stream_name] = row['result']
    return result_dict


def _get_average_da(streams_da: dict) -> dict:
    """Calculate average data availability among all data streams"""
    total_results = {}
    for k, v in streams_da.items():
        for i, j in v.items():
            if i not in total_results:
                total_results[i] = []
            total_results[i].append(j)

    return {k: math.ceil(statistics.mean(v)) for k, v in total_results.items()}


def _get_data_availability(
    ref: str,
    average: bool,
    resolution: Literal['hourly', 'daily', 'monthly'] = 'daily',
):
    url_template = 'https://raw.githubusercontent.com/ooi-data/data_availability/main/{resolution}/{ref}'.format
    request_url = url_template(resolution=resolution, ref=ref)
    response = requests.get(request_url)
    if response.status_code == 200:
        result_dict = response.json()
        if average:
            data_availability = {ref: _get_average_da(result_dict)}
        else:
            data_availability = {ref: result_dict}
    else:
        data_availability = {ref: None}
    return data_availability


def _get_inst_params(refdes):
    inst_catalog = META["instruments_catalog"]
    inst_list = list(
        filter(lambda i: i["reference_designator"] == refdes, inst_catalog)
    )
    new_inst = {
        "nameset": None,
        "idset": None,
        "products": None,
    }
    if len(inst_list) > 0:
        inst = inst_list[0]
        params = list(
            filter(
                lambda p: (p["pid"] == 7)
                or (
                    (p["data_product_identifier"] is not None)
                    and (
                        "L1" in p["data_product_identifier"]
                        or "L2" in p["data_product_identifier"]
                        or "BOTSFLU" in p["data_product_identifier"]
                    )
                ),
                inst['parameters'],
            )
        )
        if len(params) > 0:
            set_params = list(set([p["parameter_name"] for p in params]))
            param_ids = list(set([p['pid'] for p in params]))
            new_inst["nameset"] = set_params
            new_inst["idset"] = param_ids
            new_inst["products"] = params

    return new_inst


async def _create_simple_view(instrument_list):
    new_list = []
    for inst in instrument_list:
        inst_view = {}
        inst_view["reference_designator"] = inst["reference_designator"]
        inst_view["instrument_name"] = inst["instrument_name"]
        inst_view["asset_type"] = inst["asset_type"]
        if "nameset" in inst:
            nameset = inst["nameset"]
        else:
            param = _get_inst_params(inst["reference_designator"])
            nameset = param["nameset"]
        param_text = ""
        if nameset:
            param_text = "; ".join(
                list(filter(lambda i: "time" not in i.lower(), nameset))
            )
        site = await _fetch_table(
            "cava_sites",
            record=True,
            filters=[("reference_designator", "==", inst["site_rd"])],
        )[0]
        infra = await _fetch_table(
            "cava_infrastructures",
            record=True,
            filters=[("reference_designator", "==", inst["infra_rd"])],
        )[0]
        inst_view["site_name"] = site["site_name"]
        inst_view["infrastructure_name"] = infra["name"]
        inst_view["param_text"] = param_text
        new_list.append(inst_view)


def _create_column_filter(
    column_name: str, value: str
) -> Tuple[str, str, Union[str, Tuple]]:
    """
    Create disjunctive normal form (DNF) filters, based on value
    https://jorisvandenbossche.github.io/arrow-docs-preview/html-option-1/python/generated/pyarrow.parquet.read_table.html#pyarrow-parquet-read-table

    """
    value_list = value.split(',')
    if len(value_list) > 1:
        values = tuple(val.strip() for val in value_list)
        filters = (column_name, "in", values)
    else:
        filters = (column_name, "==", value_list[0])
    return filters


@router.get("/arrays")
async def get_arrays(
    version: bool = Depends(_check_version), cache=Depends(redis_dependency)
):
    if version:
        try:
            results = await _fetch_table(
                "cava_arrays", record=True, cache=cache
            )
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                results = await _fetch_table(
                    "cava_arrays", record=True, cache=None
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
    return results


@router.get("/areas")
async def get_site_areas(
    version: bool = Depends(_check_version),
    geojson: bool = True,
    cache=Depends(redis_dependency),
):
    if version:
        # for now drop empty coordinates
        try:
            tabledf = await _fetch_table("cava_areas", cache=cache)
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                tabledf = await _fetch_table("cava_areas", cache=None)
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
        tabledf = tabledf.dropna(subset=['coordinates'])
        if geojson:
            tabledf.loc[:, "geometry"] = tabledf.coordinates.apply(_get_poly)
            tabledf = tabledf.drop("coordinates", axis=1)
            gdf = gpd.GeoDataFrame(
                tabledf,
                crs={"init": "epsg:4326"},
                geometry=tabledf["geometry"],
            )
            results = json.loads(gdf.to_json())
        else:
            results = _df_to_record(tabledf)
    return results


@router.get("/infrastructures")
async def get_infrastructures(
    version: bool = Depends(_check_version), cache=Depends(redis_dependency)
):
    if version:
        try:
            results = await _fetch_table(
                "cava_infrastructures", record=True, cache=cache
            )
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                results = await _fetch_table(
                    "cava_infrastructures", record=True, cache=None
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
    return results


@router.get("/points-of-exploration")
async def get_points_of_exploration(
    version: bool = Depends(_check_version), cache=Depends(redis_dependency)
):
    if version:
        try:
            results = await _fetch_table("cava_poe", record=True, cache=cache)
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                results = await _fetch_table(
                    "cava_poe", record=True, cache=None
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
    return results


async def _get_instruments(
    instrument_request: InstrumentRequest,
    filters: Optional[List] = None,
    cache: Optional[Any] = None,
) -> List[Dict[Any, Any]]:
    results = await _fetch_table(
        "cava_instruments", record=True, filters=filters, cache=cache
    )
    if instrument_request.refdes:
        rd_list = instrument_request.refdes.strip(" ").split(",")
        results = list(
            filter(
                lambda r: r["reference_designator"] in rd_list,
                results,
            )
        )

    if instrument_request.include_params:
        final_results = [
            dict(
                **_get_inst_params(res["reference_designator"]),
                **res,
            )
            for res in results
        ]
    else:
        final_results = results

    return final_results


@router.get("/instruments")
async def get_instruments(
    instrument_request: InstrumentRequest = Depends(),
    version: bool = Depends(_check_version),
    cache: aioredis.client.Redis = Depends(redis_dependency),
):
    filters = None
    final_results = []
    if version:
        if any(
            [
                instrument_request.site,
                instrument_request.group,
                instrument_request.infrastructure,
                instrument_request.area,
            ]
        ):
            filters = []
            if instrument_request.site:
                filters.append(
                    _create_column_filter("site_rd", instrument_request.site)
                )

            if instrument_request.group:
                filters.append(
                    _create_column_filter(
                        "group_code", instrument_request.group
                    )
                )

            if instrument_request.infrastructure:
                filters.append(
                    _create_column_filter(
                        "infra_rd", instrument_request.infrastructure
                    )
                )

            if instrument_request.area:
                filters.append(
                    _create_column_filter("area_rd", instrument_request.area)
                )
        try:
            final_results = await _get_instruments(
                instrument_request, filters, cache=cache
            )

            if len(final_results) == 0:
                return JSONResponse(
                    status_code=204,
                    content={"message": "Instruments not found"},
                )
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                final_results = await _get_instruments(
                    instrument_request, filters, cache=None
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
    return final_results


@router.get("/instrument/{refdes}")
async def get_single_instrument(
    version: bool = Depends(_check_version),
    refdes: str = "",
    cache=Depends(redis_dependency),
):
    filters = None
    if version:
        if refdes:
            filters = [("reference_designator", "==", refdes)]
        try:
            results = await _fetch_table(
                "cava_instruments", record=True, filters=filters, cache=cache
            )
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                results = await _fetch_table(
                    "cava_instruments",
                    record=True,
                    filters=filters,
                    cache=None,
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )

        final_results = [
            dict(**res, **_get_inst_params(res["reference_designator"]))
            for res in results
        ]
        if len(final_results) == 1:
            return final_results[0]
        else:
            return JSONResponse(
                status_code=204,
                content={"message": f"{refdes} not found"},
            )


@router.get("/instrument/{refdes}/streams")
async def get_instrument_streams(
    version: bool = Depends(_check_version),
    refdes: str = "",
    cache=Depends(redis_dependency),
):
    filters = None
    if version:
        if refdes:
            filters = [
                ("reference_designator", "==", refdes),
                ("stream_type", "==", "Science"),
            ]

        try:
            results = await _fetch_table(
                "ooi_streams", record=True, filters=filters, cache=cache
            )
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                results = await _fetch_table(
                    "ooi_streams", record=True, filters=filters, cache=None
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
    return results


@router.get("/instrument/{refdes}/deployments")
async def get_instrument_deployments(
    version: bool = Depends(_check_version),
    refdes: str = "",
    cache=Depends(redis_dependency),
):
    deployments = []
    if version:
        try:
            cache_key = hash_dict(
                {'refdes': refdes, 'func': 'get_instrument_deployments'}
            )
            cached_result = await _get_cache(cache, cache_key)
            if cached_result is not None:
                deployments = json.loads(cached_result)
            else:
                deployments = await retrieve_deployments(refdes)

                await _set_cache(cache, cache_key, value=deployments)
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                deployments = await retrieve_deployments(refdes)
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )

    return deployments


@router.get("/instruments/groups")
async def get_instrument_groups(
    version: bool = Depends(_check_version),
    cache=Depends(redis_dependency),
):
    if version:
        try:
            results = await _fetch_table(
                "cava_instrument-groups", record=True, cache=cache
            )
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                results = await _fetch_table(
                    "cava_instrument-groups", record=True, cache=None
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
    return results


@router.get("/instruments/catalog")
async def get_insts_catalog():
    return META["instruments_catalog"]


@router.get("/data-products")
async def get_data_products(
    version: bool = Depends(_check_version),
    cache=Depends(redis_dependency),
):
    if version:
        try:
            results = await _fetch_table(
                "cava_dataproducts", record=True, cache=cache
            )
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                results = await _fetch_table(
                    "cava_dataproducts", record=True, cache=None
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
    return results


@router.get("/data-products/groups")
async def get_data_product_groups(
    version: bool = Depends(_check_version),
    cache=Depends(redis_dependency),
):
    if version:
        try:
            results = await _fetch_table(
                "cava_dataproduct-groups", record=True, cache=cache
            )
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                results = await _fetch_table(
                    "cava_dataproduct-groups", record=True, cache=None
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
    return results


@router.get("/parameters")
async def get_parameters(
    version: bool = Depends(_check_version),
    cache=Depends(redis_dependency),
):
    if version:
        try:
            results = await _fetch_table(
                "ooi_parameters", record=True, cache=cache
            )
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                results = await _fetch_table(
                    "ooi_parameters", record=True, cache=None
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
    return results


@router.get("/streams")
async def get_streams(
    version: bool = Depends(_check_version),
    inst_rd: str = "",
    cache=Depends(redis_dependency),
):
    filters = None
    if version:
        if any([inst_rd]):
            filters = [("stream_type", "==", "Science")]
            if inst_rd:
                filters.append(("reference_designator", "==", inst_rd))
        try:
            results = await _fetch_table(
                "ooi_streams", record=True, filters=filters, cache=cache
            )
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                results = await _fetch_table(
                    "ooi_streams", record=True, filters=filters, cache=None
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
    return results


@router.get("/streams/{refdes}")
async def get_single_stream(
    version: bool = Depends(_check_version),
    refdes: str = "",
    cache=Depends(redis_dependency),
):
    filters = None
    if version:
        if refdes:
            filters = [
                ("stream", "==", refdes),
                ("stream_type", "==", "Science"),
            ]
        try:
            results = await _fetch_table(
                "ooi_streams", record=True, filters=filters, cache=cache
            )
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                results = await _fetch_table(
                    "ooi_streams", record=True, filters=filters, cache=cache
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
    return results


@router.get("/sites")
async def get_sites(
    version: bool = Depends(_check_version),
    geojson: bool = True,
    cache=Depends(redis_dependency),
):
    if version:
        try:
            tabledf = await _fetch_table("cava_sites", cache=cache)
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                tabledf = await _fetch_table("cava_sites", cache=None)
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
        tabledf = tabledf[tabledf.active_display == True]  # noqa
        if geojson:
            results = json.loads(_df_to_gdf_points(tabledf).to_json())
        else:
            results = _df_to_record(tabledf)
    return results


@router.get("/get_instruments_catalog")
async def get_instruments_catalog(version: bool = Depends(_check_version)):
    if version:
        if "legacy_catalog" not in META:
            fs = fsspec.filesystem('s3')
            with fs.open(
                os.path.join('ooi-metadata', 'legacy_catalog.json')
            ) as f:
                results = json.load(f)
                META.update({"legacy_catalog": results})
        else:
            results = META["legacy_catalog"]
    return results


@router.get("/global_ranges")
async def get_global_ranges(
    version: bool = Depends(_check_version),
    cache=Depends(redis_dependency),
):
    if version:
        try:
            results = await _fetch_table(
                "global_ranges", record=True, cache=cache
            )
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                results = await _fetch_table(
                    "global_ranges", record=True, cache=None
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
    return results


@router.get("/cables")
async def get_cables(version: bool = Depends(_check_version)):
    if version:
        return RSN_CABLE


@router.get("/get_deployments", deprecated=True)
async def get_deployments(
    refdes: str = "", cache=Depends(redis_dependency)
):
    deployments = []
    try:
        cache_key = hash_dict({'refdes': refdes, 'func': 'get_deployments'})
        cached_result = await _get_cache(cache, cache_key)
        if cached_result is not None:
            deployments = json.loads(cached_result)
        else:
            deployments = await retrieve_deployments(refdes)

            await _set_cache(cache, cache_key, value=deployments)
    except Exception as e:
        if isinstance(e, ConnectionError):
            logger.error("Redis disconnected! Fetching from source.")
            deployments = await retrieve_deployments(refdes)
        else:
            raise HTTPException(
                status_code=500,
                detail={"message": f"{e}"},
            )

    return deployments


@router.get("/data_availability")
async def get_data_availability(
    ref: str,
    average: bool = True,
    resolution: Literal['hourly', 'daily', 'monthly'] = 'daily',
    version: bool = Depends(_check_version),
    cache=Depends(redis_dependency),
):
    data_availability = {}
    try:
        if version and ref:
            cache_key = hash_dict(
                {
                    'ref': ref,
                    'average': average,
                    'resolution': resolution,
                    'func': 'get_data_availability',
                }
            )
            cached_result = await _get_cache(cache, cache_key)
            if cached_result is not None:
                data_availability = json.loads(cached_result)
            else:
                data_availability = _get_data_availability(
                    ref, average, resolution=resolution
                )

                await _set_cache(cache, cache_key, value=data_availability)
    except Exception as e:
        if isinstance(e, ConnectionError):
            logger.error("Redis disconnected! Fetching from source.")
            data_availability = _get_data_availability(
                ref, average, resolution=resolution
            )
        else:
            raise HTTPException(
                status_code=500,
                detail={"message": f"{e}"},
            )

    if isinstance(data_availability[ref], dict):
        return data_availability
    else:
        return JSONResponse(
            status_code=204,
            content={"message": f"{ref} not found"},
        )


async def _retrieve_all_annotations(prepped_request):
    annotations = {"annotations": {}, "count": 0}
    count = 0
    for rd in prepped_request["rd_list"]:
        r = rd.split("-")
        refdes = "-".join(r[:4])
        stream_method = r[-2]
        stream_rd = r[-1]
        anno_df = await _get_annotations(
            reference_designator=refdes,
            stream_method=stream_method,
            stream_rd=stream_rd,
            begin_date=prepped_request["begin_date"],
            end_date=prepped_request["end_date"],
        )
        anno = []
        if isinstance(anno_df, pd.DataFrame):
            anno = json.loads(anno_df.to_json(orient="records"))
        count += len(anno)

        annotations["annotations"].update({refdes: anno})
    return annotations


@router.get("/get_annotations")
async def get_annotations(
    prepped_request: dict = Depends(_prepare_anno_request),
    version: bool = Depends(_check_version),
    cache=Depends(redis_dependency),
):
    if version:
        try:
            cache_key = hash_dict(
                {
                    'ref': prepped_request,
                    'func': 'get_annotations',
                }
            )
            cached_result = await _get_cache(cache, cache_key)
            if cached_result is not None:
                annotations = json.loads(cached_result)
            else:
                annotations = await _retrieve_all_annotations(prepped_request)
        except Exception as e:
            if isinstance(e, ConnectionError):
                logger.error("Redis disconnected! Fetching from source.")
                annotations = await _retrieve_all_annotations(prepped_request)
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"message": f"{e}"},
                )
        return annotations


@router.get("/status")
async def get_service_status():
    return {"status": "running", "message": "Metadata service is up."}
