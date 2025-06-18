import os
import sys
import subprocess

# Add current directory to Python path untuk import _scrap
sys.path.append(os.path.dirname(__file__))
from _scrap import scrape_idx_data

def collect_data():
    """Collect IDX data for year 2021 - Annual"""
    scrape_idx_data(2021, "annual")

if __name__ == "__main__":
    collect_data()
