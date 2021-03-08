import json
import os
import logging
from typing import TypeVar
import fsspec

import geopandas as gpd
import pandas as pd
import pytz
import redis
from dask import dataframe
from dateutil import parser
from shapely.geometry import Polygon
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from core.config import (
    BASE_URL,
    CURRENT_API_VERSION,
    M2M_URL,
    REDIS_HOST,
    REDIS_PORT,
    METADATA_SOURCE,
)
from store import META
from utils.conn import send_request, retrieve_deployments
from utils.parsers import parse_annotations_json, unix_time_millis
from api.cables import RSN_CABLE

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


def _fetch_table(
    table_name: str,
    record: bool = False,
    filters: TypeVar("Choosable", list, type(None)) = None,
) -> Choosable:
    tabledf = dataframe.read_parquet(
        os.path.join(METADATA_SOURCE, table_name),
        engine="pyarrow-dataset",
        filters=filters,
        index=False,
    ).compute()
    if record:
        tabledf = _df_to_record(tabledf)
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


def _get_data_availability(refdes):
    icdf = pd.DataFrame(META["catalog_list"])
    inst_list = icdf[icdf.instrument_rd.str.match(refdes)]
    res = {}
    inst = None
    if len(inst_list) == 1:
        inst = inst_list.iloc[0]
    else:
        for _, row in inst_list.iterrows():
            if (
                row["stream_rd"] == row["instrument"]["preferred_stream"]
            ) and (
                row["stream_method"]
                == row["instrument"]["preferred_stream_method"]
            ):
                inst = row

    if not isinstance(inst, type(None)):
        dest_fold = f"ooi-data/data_availability/{inst.data_table}"
        dadf = dataframe.read_parquet(
            f"s3://{dest_fold}", index=False
        ).compute()
        for idx, val in dadf.iterrows():
            res[str(val["dtindex"].astype("int64"))] = int(
                val["count"].astype("int64")
            )
    return res


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
                or (p["data_product_type"] is not None)
                and (p["data_product_type"] == 'Science Data')
                and (p["data_product_identifier"] is not None)
                and (
                    "L1" in p["data_product_identifier"]
                    or "L2" in p["data_product_identifier"]
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


def _create_simple_view(instrument_list):
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
        site = _fetch_table(
            "cava_sites",
            record=True,
            filters=[("reference_designator", "==", inst["site_rd"])],
        )[0]
        infra = _fetch_table(
            "cava_infrastructures",
            record=True,
            filters=[("reference_designator", "==", inst["infra_rd"])],
        )[0]
        inst_view["site_name"] = site["site_name"]
        inst_view["infrastructure_name"] = infra["name"]
        inst_view["param_text"] = param_text
        new_list.append(inst_view)


@router.get("/arrays")
def get_arrays(version: bool = Depends(_check_version)):
    if version:
        results = _fetch_table("cava_arrays", record=True)
    return results


@router.get("/areas")
def get_site_areas(
    version: bool = Depends(_check_version), geojson: bool = True
):
    if version:
        # for now drop empty coordinates
        tabledf = _fetch_table("cava_areas")
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
def get_infrastructures(version: bool = Depends(_check_version)):
    if version:
        results = _fetch_table("cava_infrastructures", record=True)
    return results


@router.get("/instruments")
def get_instruments(
    version: bool = Depends(_check_version),
    site: str = None,
    group: str = None,
    infrastructure: str = None,
    area: str = None,
    include_params: bool = False,
    refdes: str = None,
):
    filters = None
    final_results = []
    if version:
        if any([site, group, infrastructure, area]):
            filters = []
            if site:
                filters.append(("site_rd", "==", site))

            if group:
                filters.append(("group_code", "==", group))

            if infrastructure:
                filters.append(("infra_rd", "==", infrastructure))

            if area:
                filters.append(("area_rd", "==", area))
        try:
            results = _fetch_table(
                "cava_instruments", record=True, filters=filters
            )
            if refdes:
                rd_list = refdes.strip(" ").split(",")
                results = list(
                    filter(
                        lambda r: r["reference_designator"] in rd_list, results
                    )
                )

            if include_params:
                final_results = [
                    dict(
                        **_get_inst_params(res["reference_designator"]), **res
                    )
                    for res in results
                ]
            else:
                final_results = results

            if len(final_results) == 0:
                return JSONResponse(
                    status_code=204,
                    content={"message": "Instruments not found"},
                )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={"message": f"{e}"},
            )
    return final_results


@router.get("/instrument/{refdes}")
def get_single_instrument(
    version: bool = Depends(_check_version), refdes: str = ""
):
    filters = None
    if version:
        if refdes:
            filters = [("reference_designator", "==", refdes)]
        try:
            results = _fetch_table(
                "cava_instruments", record=True, filters=filters
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
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={"message": f"{e}"},
            )


@router.get("/instrument/{refdes}/streams")
def get_instrument_streams(
    version: bool = Depends(_check_version), refdes: str = ""
):
    filters = None
    if version:
        if refdes:
            filters = [
                ("reference_designator", "==", refdes),
                ("stream_type", "==", "Science"),
            ]

        results = _fetch_table("ooi_streams", record=True, filters=filters)
    return results


@router.get("/instrument/{refdes}/deployments")
async def get_instrument_deployments(
    version: bool = Depends(_check_version), refdes: str = ""
):
    deployments = []
    if version:
        deployments = await retrieve_deployments(refdes)

    return deployments


@router.get("/instruments/groups")
def get_instrument_groups(version: bool = Depends(_check_version)):
    if version:
        results = _fetch_table("cava_instrument-groups", record=True)
    return results


@router.get("/instruments/catalog")
async def get_insts_catalog():
    return META["instruments_catalog"]


@router.get("/data-products")
def get_data_products(version: bool = Depends(_check_version)):
    if version:
        results = _fetch_table("cava_dataproducts", record=True)
    return results


@router.get("/data-products/groups")
def get_data_product_groups(version: bool = Depends(_check_version)):
    if version:
        results = _fetch_table("cava_dataproduct-groups", record=True)
    return results


@router.get("/parameters")
def get_parameters(version: bool = Depends(_check_version)):
    if version:
        results = _fetch_table("ooi_parameters", record=True)
    return results


@router.get("/streams")
def get_streams(version: bool = Depends(_check_version), inst_rd: str = ""):
    filters = None
    if version:
        if any([inst_rd]):
            filters = [("stream_type", "==", "Science")]
            if inst_rd:
                filters.append(("reference_designator", "==", inst_rd))
        results = _fetch_table("ooi_streams", record=True, filters=filters)
    return results


@router.get("/streams/{refdes}")
def get_single_stream(
    version: bool = Depends(_check_version), refdes: str = ""
):
    filters = None
    if version:
        if refdes:
            filters = [
                ("stream", "==", refdes),
                ("stream_type", "==", "Science"),
            ]
        results = _fetch_table("ooi_streams", record=True, filters=filters)
    return results


@router.get("/sites")
def get_sites(version: bool = Depends(_check_version), geojson: bool = True):
    if version:
        tabledf = _fetch_table("cava_sites")
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
def get_global_ranges(version: bool = Depends(_check_version)):
    if version:
        results = _fetch_table("global_ranges", record=True)

    return results


@router.get("/cables")
async def get_cables(version: bool = Depends(_check_version)):
    if version:
        return RSN_CABLE


@router.get("/get_deployments")
def get_deployments(version: bool = Depends(_check_version), refdes: str = ""):
    # deployments = list(
    #     filter(
    #         lambda dep: dep["reference_designator"] == refdes,
    #         META["deployments_list"],
    #     )
    # )
    deployments = retrieve_deployments(refdes)

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
    prepped_request: dict = Depends(_prepare_anno_request),
    version: bool = Depends(_check_version),
):
    if version:
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
        annotations["count"] = count
        return annotations


@router.get("/status")
async def get_service_status():
    return {"status": "running", "message": "Metadata service is up."}
