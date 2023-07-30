import logging
import os
from queue import Queue
import sys

from typing import Callable, Dict, List, Optional

import validators

from utils.url import get_base_url, get_cleaned_url, get_filename

# where to store the downloaded pages by default
DEFAULT_OUTPUT_DIR = os.path.join("web-wanderer", "downloads")


class Crawler:
    def __init__(
        self,
        seed_url: str,
        output_dir: Optional[str] = None,
        done_callback: Optional[Callable] = None,
    ):
        if isinstance(validators.url(seed_url), validators.ValidationFailure):
            raise ValueError(f"invalid seed_url '{seed_url}'")

        # the url to start crawling from
        self.seed_url = get_cleaned_url(seed_url)

        # the base url of the seed
        self.base_url = get_base_url(seed_url)

        # where to store the downloaded pages
        self.output_dir = os.path.join(
            output_dir or DEFAULT_OUTPUT_DIR, get_filename(self.base_url)
        )

        # where the downloaded pages are to be stored
        os.makedirs(self.output_dir, exist_ok=True)

        # shared queue to store URLs to be crawled
        self.url_queue = Queue()

        # set to keep track of crawled URLs
        self.crawled_urls = set()

        # set of URLs that weren't crawled successfully
        self.failed_urls = set()

        # set of URLs that are already distributed to a worker
        self.distrubuted_urls = set()

        # a callback function that will be called after crawling is SUCCESSFULLY done
        self.done_callback = done_callback

        # instantiate a Logger for this class
        self.logger = self.get_logger()

        # a JSON array of metadata [{"url": "<URL>", "filename": "<FILENAME>"}]
        self.metadata: Dict = {"successful": [], "failed": []}

    def get_logger(self):
        """
        Returns a Logger instance with this class's name.
        """

        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)

        # create console handler
        ch = logging.StreamHandler(sys.stdout)

        # create formatter for console handler
        formatter = logging.Formatter(
            fmt="%(asctime)s %(name)s (%(levelname)s) %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
        )
        ch.setFormatter(formatter)

        logger.addHandler(ch)
        return logger

    def start(self):
        raise NotImplementedError

    def should_crawl(self, url: str):
        """
        Returns True if the given URL
            - starts with seed_url
            - AND NOT already distrubuted
            - AND NOT already crawled
            - AND NOT failed\n
        else False.
        """

        url = get_cleaned_url(url)

        return (
            url.startswith(self.seed_url)
            and url not in self.distrubuted_urls
            and url not in self.crawled_urls
            and url not in self.failed_urls
        )
