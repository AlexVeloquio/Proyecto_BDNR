import argparse
import logging
import os
import requests


# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('books.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

BOOKS_API_URL = os.getenv("BOOKS_API_URL", "http://localhost:27017")

def main():
    log.info(f"Welcome to books catalog. App requests to: {BOOKS_API_URL}")
