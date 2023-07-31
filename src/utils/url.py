import re

import validators

from urllib.parse import urlparse, urljoin


def is_url(s: str):
    return not isinstance(validators.url(s), validators.ValidationFailure)


def get_base_url(url):
    if type(url) != str:
        return None

    parsed_url = urlparse(url)

    if not all([parsed_url.scheme, parsed_url.netloc]):
        return None

    return f"{parsed_url.scheme}://{parsed_url.netloc}"


def add_base_url(path, base_url):
    return urljoin(base_url, path)


def get_cleaned_url(url):
    if type(url) != str:
        return None

    parsed_url = urlparse(url)

    if not all([parsed_url.scheme, parsed_url.netloc]):
        return None

    url_path = re.sub(r"/+", "/", parsed_url.path)
    if url_path.endswith("/"):
        url_path = url_path[:-1]

    return f"{parsed_url.scheme}://{parsed_url.netloc}{url_path}"


def get_filename(url, skip_base_url=False):
    if type(url) != str:
        return None

    parsed_url = urlparse(url)

    if not all([parsed_url.scheme, parsed_url.netloc]):
        return None

    if skip_base_url:
        filename = parsed_url.path
    else:
        filename = f"{parsed_url.netloc}{parsed_url.path}"

    filename = re.sub(r"/+", "_", filename)

    if filename.startswith("_"):
        filename = filename[1:]

    if filename.endswith("_"):
        filename = filename[:-1]

    if len(filename) == 0:
        return "index"

    return filename
