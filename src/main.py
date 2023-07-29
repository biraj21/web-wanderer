import sys

from crawlers import MultithreadedCrawler

if len(sys.argv) < 2:
    print(f"usage: {sys.argv[0]} URL")
    exit(1)


try:
    crawler = MultithreadedCrawler(sys.argv[1])
    crawler.start()
except Exception as e:
    print(f"error: {e}")
