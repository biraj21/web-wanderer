from urllib.parse import urlparse, urljoin


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

    return f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"


def get_filename(url, skip_base_url=False):
    parsed_url = urlparse(url)

    if not all([parsed_url.scheme, parsed_url.netloc]):
        return None

    if skip_base_url and not parsed_url.path:
        raise Exception("url needs path when skip_base_url=True")

    if skip_base_url:
        filename = parsed_url.path
    else:
        filename = f"{parsed_url.netloc}{parsed_url.path}"

    filename = filename.replace("/", "_")

    if filename.startswith("_"):
        filename = filename[1:]

    if filename.endswith("_"):
        filename = filename[:-1]

    return filename
