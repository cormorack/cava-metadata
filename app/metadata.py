import os

from flask import (
    Blueprint,
    flash,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
    current_app,
)
import json
from werkzeug.security import check_password_hash, generate_password_hash
from flask import Response
from app.db import get_db
import redis
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import requests
from yodapy.utils.parser import parse_annotations_json, unix_time_millis
from yodapy.utils.conn import fetch_url
from dateutil import parser
from dask import dataframe
from dask.diagnostics import ProgressBar
import pytz

from typing import Dict, List, TypeVar

from .creator import initialize_metadata

bp = Blueprint("metadata", __name__, url_prefix="/metadata")

Choosable = TypeVar("Choosable", str, pd.DataFrame)

BASE_URL = "https://ooinet.oceanobservatories.org"
M2M_URL = "api/m2m"

BUCKET_NAME = os.environ.get("DATA_BUCKET", "io2data-test")

OOI_USERNAME = os.environ["OOI_USERNAME"]
OOI_TOKEN = os.environ["OOI_TOKEN"]
OLD_CAVA_API_BASE = os.environ["OLD_CAVA_API_BASE"]

META_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'meta')
METADATA = initialize_metadata(META_PATH)

try:
    redis_cache = redis.Redis(
        host=os.environ["REDIS_HOST"], port=int(os.environ["REDIS_PORT"]), db=0
    )
except Exception as e:
    print(e)

CURRENT_API_VERSION = 2.0


def _get_annotations(
    reference_designator, stream_method, stream_rd, begin_date, end_date
):
    """ Get annotations of the inst pandas Series object """
    rsession = requests.Session()
    OOI_M2M_ANNOTATIONS = (
        "https://ooinet.oceanobservatories.org/api/m2m/12580/anno/find"
    )
    params = {
        "beginDT": unix_time_millis(
            parser.parse(begin_date).replace(tzinfo=pytz.UTC)
        ),  # noqa
        "endDT": unix_time_millis(
            parser.parse(end_date).replace(tzinfo=pytz.UTC)
        ),  # noqa
        "method": stream_method,
        "refdes": reference_designator,
        "stream": stream_rd,
    }
    rannotations = fetch_url(
        requests.Request(
            "GET",
            OOI_M2M_ANNOTATIONS,
            auth=(os.environ.get("OOI_USERNAME"), os.environ.get("OOI_TOKEN")),
            params=params,
        ).prepare(),
        session=rsession,
    )
    try:
        parsed_annotations = parse_annotations_json(rannotations.json())
        return parsed_annotations
    except Exception:
        return rannotations.status_code


def _get_global_ranges():
    dest_fold = f"{BUCKET_NAME}/metadata/global-ranges"
    ddf = dataframe.read_parquet(f"s3://{dest_fold}", index=False)
    rangesdf = ddf.compute()
    return rangesdf.to_json(orient="records")


def _get_data_availability(foldername):
    icdf = pd.DataFrame(METADATA['catalog_list'])
    inst_list = icdf[icdf.instrument_rd.str.match(foldername)]
    res = {}
    inst = None
    if len(inst_list) == 1:
        inst = inst_list.iloc[0]
    else:
        for idx, row in inst_list.iterrows():
            if (
                row["stream_rd"] == row["instrument"]["preferred_stream"]
            ) and (
                row["stream_method"]
                == row["instrument"]["preferred_stream_method"]
            ):
                inst = row

    if not isinstance(inst, type(None)):
        dest_fold = f"ooi-data/data-availability/{inst.data_table}"
        with ProgressBar():
            dadf = dataframe.read_parquet(
                f"s3://{dest_fold}", index=False
            ).compute()
            for idx, val in dadf.iterrows():
                res[str(val["dtindex"].astype("int64"))] = int(
                    val["count"].astype("int64")
                )
    return res


def _fetch_table(table_name: str, record: bool = False) -> Choosable:
    # db = get_db()
    # tabledf = pd.read_sql_table(table_name, con=db).fillna("")
    tabledf = METADATA['dfdict'][table_name]
    if record:
        tabledf = _df_to_record(tabledf)
    return tabledf


def _fetch_catalog(page: int, limit: int, record: bool = False) -> Choosable:
    if limit == -1 or page == -1:
        tabledf = _fetch_table(table_name="catalog", record=record)
    else:
        db = get_db()
        offset = limit * (page - 1)
        sql = f"""SELECT *
      FROM catalog
      ORDER BY "id"
      LIMIT {limit}
      OFFSET {offset};
        """
        tabledf = pd.read_sql(sql, con=db)
        if record:
            tabledf = _df_to_record(tabledf)
    return tabledf


def _fetch_table_count(table_name: str) -> int:
    db = get_db()
    return db.execute(f"SELECT COUNT(*) FROM {table_name}").first()[0]


def _df_to_record(df: pd.DataFrame) -> str:
    return df.to_json(orient="records")


def _df_to_gdf_points(df: pd.DataFrame) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        df,
        crs={"init": "epsg:4326"},
        geometry=gpd.points_from_xy(df["lon"], df["lat"]),
    )


def _send_request(url, params=None, timeout=None):
    SESSION = requests.Session()
    ADAPTER = requests.adapters.HTTPAdapter(max_retries=0)
    SESSION.mount("http://", ADAPTER)
    SESSION.mount("https://", ADAPTER)
    r = SESSION.get(
        url, auth=(OOI_USERNAME, OOI_TOKEN), params=params, timeout=timeout
    )
    if r.status_code == 200:
        try:
            return r.json()
        except Exception as e:
            print(e)
            return r.text
    else:
        print(r.status_code)


def _get_poly(row):
    return Polygon(json.loads(row))


def _retrieve_site_annotations(site: Dict) -> List[Dict]:
    try:
        annot = _send_request(
            "/".join([BASE_URL, M2M_URL, str(12580), "anno/find"]),
            params={"refdes": site["reference_designator"]},
            timeout=1,
        )
        if isinstance(annot, list):
            anndf = pd.DataFrame(annot)
            if len(anndf) > 0:
                site_annot = anndf[
                    anndf["stream"].isna() & anndf["node"].isna()
                ].copy()
                if len(site_annot) > 0:
                    site_annot.loc[:, "reference_designator"] = site[
                        "reference_designator"
                    ]
                    site_annot = site_annot[
                        [
                            "reference_designator",
                            "subsite",
                            "node",
                            "sensor",
                            "method",
                            "stream",
                            "parameters",
                            "beginDT",
                            "endDT",
                            "annotation",
                            "id",
                            "source",
                            "qcFlag",
                            "exclusionFlag",
                        ]
                    ]
                    site_annot = site_annot.fillna("")
                    site_annot = site_annot.rename(
                        {
                            "beginDT": "start_date",
                            "endDT": "end_date",
                            "annotation": "comment",
                            "qcFlag": "flag",
                            "exclusionFlag": "exclude",
                        },
                        axis=1,
                    )
                    return site_annot.to_dict(orient="records")
        return []
    except Exception as e:
        current_app.logger.warning(str(e))
        return []


def _retrieve_site_area(dfdict: Dict, site: Dict) -> Dict:
    areas = dfdict["areas"]
    arrays = dfdict["arrays"]
    area = (
        areas[areas.reference_designator.str.contains(site["area_rd"])]
        .iloc[0]
        .to_dict()
    )
    array = (
        arrays[arrays.reference_designator.str.contains(area["array_rd"])]
        .iloc[0]
        .to_dict()
    )
    area.update({"array": array})
    area.pop("array_rd")
    area.update({"wp_page": int(area["wp_page"])})
    return area


def _retrieve_instruments(dfdict: Dict, infrastructure: Dict) -> List[Dict]:
    inst = dfdict["instruments"]
    return inst[
        inst.reference_designator.str.contains(
            infrastructure["reference_designator"]
        )
    ].to_dict(orient="records")


def _retrieve_site_infrastructures_and_instruments(
    dfdict: Dict, site: Dict
) -> List[Dict]:
    infra = dfdict["infrastructures"]
    infrastructures = infra[
        infra.reference_designator.str.contains(site["reference_designator"])
    ].to_dict(orient="records")

    infrastructure_list = []
    for infrastructure in infrastructures:
        instruments = _retrieve_instruments(dfdict, infrastructure)
        infrastructure.update({"instruments": instruments})
        infrastructure_list.append(infrastructure)

    return infrastructure_list


@bp.route("/status")
def get_server_status():
    return "Metadata service is up."


@bp.route("/areas")
def get_site_areas():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    geojson = request.args.get("geojson", "true", type=bool)
    if version == CURRENT_API_VERSION:
        # for now drop empty coordinates
        tabledf = _fetch_table("areas").dropna(subset=['coordinates'])
        if geojson == "true":
            tabledf.loc[:, "geometry"] = tabledf.coordinates.apply(_get_poly)
            tabledf = tabledf.drop("coordinates", axis=1)
            gdf = gpd.GeoDataFrame(
                tabledf,
                crs={"init": "epsg:4326"},
                geometry=tabledf["geometry"],
            )
            results = gdf.to_json()
        else:
            results = _df_to_record(tabledf)
    else:
        results = ""
    return Response(results, mimetype="application/json")


@bp.route("/sites")
def get_sites():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    geojson = request.args.get("geojson", "true", type=bool)
    if version == CURRENT_API_VERSION:
        tabledf = _fetch_table("sites")
        tabledf = tabledf[tabledf.active_display == True]
        if geojson == "true":
            results = _df_to_gdf_points(tabledf).to_json()
        else:
            results = _df_to_record(tabledf)
    elif version == 1.1:
        resp = requests.get(f"{OLD_CAVA_API_BASE}/v1_1/sites").json()
        results = json.dumps(resp)
    else:
        results = ""
    return Response(results, mimetype="application/json")


@bp.route("/get_annotations")
def get_annotations():
    # TODO: Add an indicator of M2M Being down when annotations count is 0
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    params = request.args
    rd_list = params.get("ref", "").split(",")
    # stream_method = params.get("stream_method", "")
    # stream_rd = params.get("stream_ref", "")
    begin_date = params.get("start_dt", "")
    end_date = params.get("end_dt", "")
    annotations = {"annotations": {}, "count": 0}
    if version == CURRENT_API_VERSION:
        count = 0
        for rd in rd_list:
            r = rd.split("-")
            refdes = "-".join(r[:4])
            stream_method = r[-2]
            stream_rd = r[-1]
            anno_df = _get_annotations(
                reference_designator=refdes,
                stream_method=stream_method,
                stream_rd=stream_rd,
                begin_date=begin_date,
                end_date=end_date,
            )
            anno = []
            if isinstance(anno_df, pd.DataFrame):
                anno = json.loads(anno_df.to_json(orient="records"))
            count += len(anno)

            annotations["annotations"].update({refdes: anno})
        annotations["count"] = count

    return Response(json.dumps(annotations), mimetype="application/json")


@bp.route("/global_ranges")
def get_global_ranges():
    global_ranges = METADATA['global_ranges'].to_json(orient='records')
    # version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    # if version == CURRENT_API_VERSION:
    #     global_ranges = _get_global_ranges()
    # else:
    #     global_ranges = []

    return Response(global_ranges, mimetype="application/json")


@bp.route("/get_deployments")
def get_deployments():
    params = request.args
    refdes = params.get("refdes", "")
    deployments = list(
        filter(
            lambda dep: dep['reference_designator'] == refdes,
            METADATA['deployments_list'],
        )
    )

    return Response(json.dumps(deployments), mimetype="application/json")


@bp.route("/data_availability")
def get_data_availability():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    params = request.args
    refdes = params.get("ref", "")
    if version == CURRENT_API_VERSION:
        data_availability_res = _get_data_availability(refdes)
    else:
        data_availability_res = {}

    if refdes:
        data_availability = {refdes: data_availability_res}
    else:
        data_availability = {}

    return Response(json.dumps(data_availability), mimetype="application/json")


@bp.route("/get_instruments_catalog")
def get_instruments_catalog():
    # version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    # params = request.args
    # if version == CURRENT_API_VERSION:
    #     icdf = dataframe.read_json(
    #         f"s3://{BUCKET_NAME}/metadata/instruments-catalog/*.part"
    #     )
    #     res = icdf.compute().to_json(orient="records")
    # elif version == 1.1:
    #     res = json.dumps(requests.get(f"{OLD_CAVA_API_BASE}/v1_1/catalog").json())
    # else:
    #     res = []
    res = json.dumps(METADATA['catalog_list'])
    return Response(res, mimetype="application/json")


@bp.route("/get_site_list")
def get_site_list():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    if version == CURRENT_API_VERSION:
        try:
            results = redis_cache.get("site-list")
            if results:
                current_app.logger.info("Retrieved from cache...")
                site_list = json.loads(results)
            else:
                current_app.logger.info("Not cached.")
                dfdict = {
                    "infrastructures": _fetch_table("infrastructures"),
                    "instruments": _fetch_table("instruments"),
                    "areas": _fetch_table("areas"),
                    "arrays": _fetch_table("arrays"),
                }
                sitesdf = _fetch_table("sites")
                sites = sitesdf[sitesdf.active_display == True].to_dict(
                    orient="records"
                )
                site_list = {}
                for site in sites:
                    site_annot = _retrieve_site_annotations(site)
                    site.update({"annotations": site_annot})

                    infrastructure_list = _retrieve_site_infrastructures_and_instruments(
                        dfdict, site
                    )
                    site.update({"infrastructures": infrastructure_list})

                    site_area = _retrieve_site_area(dfdict, site)
                    site.update({"site_area": site_area})
                    site.pop("area_rd")

                    site_list[site["reference_designator"]] = site

                redis_cache.set("site-list", json.dumps(site_list), ex=3600)
        except Exception as e:
            current_app.logger.error(f"Exception occured: {e}", exc_info=True)
    else:
        site_list = {}

    return Response(json.dumps(site_list), mimetype="application/json")


@bp.route("/arrays")
def get_arrays():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    if version == CURRENT_API_VERSION:
        results = _fetch_table("arrays", record=True)
    else:
        results = ""
    return Response(results, mimetype="application/json")


@bp.route("/infrastructures")
def get_infrastructures():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    if version == CURRENT_API_VERSION:
        results = _fetch_table("infrastructures", record=True)
    else:
        results = ""
    return Response(results, mimetype="application/json")


@bp.route("/instruments")
def get_instruments():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    if version == CURRENT_API_VERSION:
        results = _fetch_table("instruments", record=True)
    else:
        results = ""
    return Response(results, mimetype="application/json")


@bp.route("/streams")
def get_streams():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    if version == CURRENT_API_VERSION:
        results = _fetch_table("streams", record=True)
    else:
        results = ""
    return Response(results, mimetype="application/json")


@bp.route("/parameters")
def get_parameters():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    if version == CURRENT_API_VERSION:
        results = _fetch_table("parameters", record=True)
    else:
        results = ""
    return Response(results, mimetype="application/json")


@bp.route("/catalog")
def get_catalog():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    limit = request.args.get("limit", 20, type=int)
    page = request.args.get("page", 1, type=int)
    if version == CURRENT_API_VERSION:
        results = json.loads(
            _fetch_catalog(page=page, limit=limit, record=True)
        )
        if limit == -1 or page == -1:
            limit = len(results)
            page = "all"
        resp = {
            "count": _fetch_table_count("catalog"),
            "page": page,
            "limit": limit,
            "results": results,
        }
    elif version == 1.1:
        resp = requests.get(f"{OLD_CAVA_API_BASE}/v1_1/catalog").json()
    else:
        resp = {}

    return Response(json.dumps(resp), mimetype="application/json")


@bp.route("/cables")
def get_cables():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    if version == CURRENT_API_VERSION:
        with open(os.path.join("/opt/app", "RSNCable.geojson")) as f:
            results = f.read()
    elif version == 1.1:
        resp = requests.get(f"{OLD_CAVA_API_BASE}/v1_1/cables").json()
        results = json.dumps(resp)
    else:
        results = ""
    return Response(results, mimetype="application/json")
