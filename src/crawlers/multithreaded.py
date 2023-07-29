from concurrent.futures import ThreadPoolExecutor
import os
import time

from typing import Callable, Optional

from playwright.sync_api import sync_playwright
from playwright._impl._api_types import TimeoutError

from .base import Crawler
from utils.url import add_base_url, get_base_url, get_cleaned_url, get_filename

# number of threads for crawling
NUM_THREADS = 8

# milliseconds to wait for 'networkidle'
PAGE_WAIT_TIMEOUT = 10_000.0


class MultithreadedCrawler(Crawler):
    def __init__(
        self,
        seed_url: str,
        output_dir: Optional[str] = None,
        done_callback: Optional[Callable] = None,
        num_threads=NUM_THREADS,
    ):
        super().__init__(seed_url, output_dir, done_callback)

        # number of threads this crawler should use
        self.num_threads = num_threads

        # set of URLs that are already distributed to a worker
        self.distrubuted_urls = set()

    # method to initiate crawling
    def start(self):
        try:
            self.url_queue.put(self.seed_url)

            start_time = time.time()

            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                futures = set()
                while not self.url_queue.empty():
                    url = self.url_queue.get()

                    # skip this URL is it's already distributed or crawling failed
                    if url in self.distrubuted_urls or url in self.failed_urls:
                        continue

                    # sechedule the crawl method for this URL
                    future = executor.submit(self.crawl, url)
                    futures.add(future)

                    # mark this URL as distributed
                    self.distrubuted_urls.add(url)

                    # queue maybe empty right now but it may get more URLs from the running threads
                    if self.url_queue.empty():
                        while any(future.running() for future in futures):
                            continue
                        else:
                            futures.clear()

            if callable(self.done_callback):
                self.done_callback()
        except Exception as e:
            self.logger.critical(f"start error: {e}")
        finally:
            print(
                f"downloaded {len(os.listdir(self.output_dir))} pages in {round(time.time() - start_time, 2)} seconds\n"
                f"downloads saved to '{self.output_dir}'\n"
                f"failed to crawl {len(self.failed_urls)} URLs"
            )

    # function to crawl a URL
    def crawl(self, url: str):
        url = add_base_url(url, base_url=self.base_url)
        current_url = url

        if (
            not url.startswith(self.seed_url)
            or get_cleaned_url(url) in self.crawled_urls
        ):
            return

        self.logger.info(f"crawling {url}")

        # launch the Chrome browser in headless mode
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch()
                page = browser.new_page()

                try:
                    page.goto(url)
                    page.wait_for_load_state("networkidle", timeout=PAGE_WAIT_TIMEOUT)
                except TimeoutError:
                    self.logger.warning(
                        f"{current_url} timeout - using whatever's rendered"
                    )

                # in case of redirects, current URL would be different from the given URL
                current_url = get_cleaned_url(page.url)

                # if the seed URL itself redirects to some other page, then handle that
                if url == self.seed_url and current_url != self.seed_url:
                    self.seed_url = get_base_url(current_url)
                    self.base_url = get_base_url(current_url)

                if (
                    not current_url.startswith(self.seed_url)
                    or current_url in self.crawled_urls
                ):
                    return

                # save the rendered HTML to a file
                html = page.inner_html("html")
                file_path = os.path.join(
                    self.output_dir,
                    f"{get_filename(current_url, skip_base_url=True)}.html",
                )

                with open(file_path, "w") as html_file:
                    html_file.write(html)

                # mark both current & redirected URLs as crawled
                self.crawled_urls.add(current_url)
                self.crawled_urls.add(url)

                # find all <a> tags
                links = page.query_selector_all("a[href]")

                # extract href attributes & remove duplicates
                hrefs = set(
                    get_cleaned_url(
                        add_base_url(link.get_attribute("href"), self.base_url)
                    )
                    for link in links
                )

                hrefs.discard(None)

                # filter out URLs that do not start with seed url or have already been crawled
                hrefs = filter(
                    lambda href: href.startswith(self.seed_url)
                    and href not in self.crawled_urls,
                    hrefs,
                )

                count = 0
                # add new URLs to the queue for crawling
                for href in hrefs:
                    self.url_queue.put(href)
                    count += 1

                self.logger.info(
                    f"{current_url} -> found {len(links)} <a> tags, to crawl {count} URLs"
                )
            except Exception as e:
                self.failed_urls.add(current_url)
                self.failed_urls.add(url)
                self.logger.error(f"error for url {current_url} - {e}", exc_info=True)
            finally:
                # close the browser to free up resources
                browser.close()
