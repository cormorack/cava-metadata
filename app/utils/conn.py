import concurrent
import logging

import requests

from ..core.config import TOKEN, USERNAME

logger = logging.getLogger(__name__)

SESSION = requests.Session()
a = requests.adapters.HTTPAdapter(max_retries=1000, pool_connections=1000, pool_maxsize=1000)
SESSION.mount("https://", a)


def map_concurrency(func, iterator, args=(), max_workers=10):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(func, i, *args): i for i in iterator}
        for future in concurrent.futures.as_completed(future_to_url):
            data = future.result()
            results.append(data)
    return results


# OOI UTILITY FUNCTIONS ==========================
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
        logger.warning(str(e))
        return None
