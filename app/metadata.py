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
import pandas as pd
import requests

from typing import Dict, List, TypeVar

bp = Blueprint("metadata", __name__, url_prefix="/metadata")

Choosable = TypeVar("Choosable", str, pd.DataFrame)

BASE_URL = "https://ooinet.oceanobservatories.org"
M2M_URL = "api/m2m"

os.environ["OOI_USERNAME"] = "OOIAPI-2OUST06WGJPVR1"
os.environ["OOI_TOKEN"] = "GHZEG56M9Q"
os.environ["OLD_CAVA_API_BASE"] = 'https://api.interactiveoceans.washington.edu'

OOI_USERNAME = os.environ["OOI_USERNAME"]
OOI_TOKEN = os.environ["OOI_TOKEN"]
OLD_CAVA_API_BASE = os.environ["OLD_CAVA_API_BASE"]

CURRENT_API_VERSION = 2.0


def _fetch_table(table_name: str, record: bool = False) -> Choosable:
    db = get_db()
    tabledf = pd.read_sql_table(table_name, con=db)
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


def _send_request(url, params=None):
    r = requests.get(url, auth=(OOI_USERNAME, OOI_TOKEN), params=params)
    if r.status_code == 200:
        try:
            return r.json()
        except Exception as e:
            print(e)
            return r.text
    else:
        print(r.status_code)


def _retrieve_site_annotations(site: Dict) -> List[Dict]:
    annot = _send_request(
        "/".join([BASE_URL, M2M_URL, str(12580), "anno/find"]),
        params={"refdes": site["reference_designator"]},
    )

    if isinstance(annot, list):
        anndf = pd.DataFrame(annot)
        if len(anndf) > 0:
            site_annot = anndf[anndf["stream"].isna() & anndf["node"].isna()].copy()
            if len(site_annot) > 0:
                site_annot.loc[:, "reference_designator"] = site["reference_designator"]
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
    return area


def _retrieve_instruments(dfdict: Dict, infrastructure: Dict) -> List[Dict]:
    inst = dfdict["instruments"]
    return inst[
        inst.reference_designator.str.contains(infrastructure["reference_designator"])
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


@bp.route("/sites")
def get_sites():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    if version == CURRENT_API_VERSION:
        results = _fetch_table("sites", record=True)
    elif version == 1.1:
        resp = requests.get(
            f"{OLD_CAVA_API_BASE}/v1_1/sites"
        ).json()
        results = json.dumps(resp)
    else:
        results = ""
    return Response(results, mimetype="application/json")


@bp.route("/get_site_list")
def get_site_list():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    if version == CURRENT_API_VERSION:
        dfdict = {
            "infrastructures": _fetch_table("infrastructures"),
            "instruments": _fetch_table("instruments"),
            "areas": _fetch_table("areas"),
            "arrays": _fetch_table("arrays"),
        }
        sitesdf = _fetch_table("sites")
        sites = sitesdf[sitesdf.active_display == True].to_dict(orient="records")
        site_list = []
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

            site_list.append(site)
    else:
        site_list = []

    return Response(json.dumps(site_list), mimetype="application/json")


@bp.route("/arrays")
def get_arrays():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    if version == CURRENT_API_VERSION:
        results = _fetch_table("arrays", record=True)
    else:
        results = ""
    return Response(results, mimetype="application/json")


@bp.route("/areas")
def get_areas():
    version = request.args.get("ver", CURRENT_API_VERSION, type=float)
    if version == CURRENT_API_VERSION:
        results = _fetch_table("areas", record=True)
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
        results = json.loads(_fetch_catalog(page=page, limit=limit, record=True))
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
        resp = requests.get(
            f"{OLD_CAVA_API_BASE}/v1_1/catalog"
        ).json()
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
        resp = requests.get(
            f"{OLD_CAVA_API_BASE}/v1_1/cables"
        ).json()
        results = json.dumps(resp)
    else:
        results = ""
    return Response(results, mimetype="application/json")
