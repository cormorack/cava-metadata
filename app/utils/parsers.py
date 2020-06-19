import datetime

import pandas as pd
import pytz


def unix_time_millis(dt):
    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)
    return int((dt - epoch).total_seconds() * 1000)


def parse_annotations_json(anno_json):
    """ Clean up annotations json """
    annodf = pd.DataFrame(anno_json).copy()
    annodf.loc[:, "begin_date"] = annodf.beginDT.apply(lambda t: pd.to_datetime(t, unit="ms"))
    annodf.loc[:, "end_date"] = annodf.endDT.apply(lambda t: pd.to_datetime(t, unit="ms"))

    # Sort descending by end date
    annodf_sorted = annodf.sort_values(by=["end_date"], ascending=False)
    annodf_sorted = annodf_sorted.drop(["beginDT", "endDT", "@class"], axis=1)
    annodf_sorted = annodf_sorted.rename(
        {
            "exclusionFlag": "exclusion_flag",
            "qcFlag": "qc_flag",
            "sensor": "instrument_rd",
            "stream": "stream_rd",
            "node": "infrastructure_rd",
            "subsite": "site_rd",
            "method": "stream_method",
        },
        axis="columns",
    )
    annodf_sorted = annodf_sorted[
        [
            "id",
            "site_rd",
            "infrastructure_rd",
            "instrument_rd",
            "annotation",
            "begin_date",
            "end_date",
            "source",
            "stream_rd",
            "stream_method",
            "parameters",
            "qc_flag",
            "exclusion_flag",
        ]
    ]
    return annodf_sorted.reset_index(drop="index")
