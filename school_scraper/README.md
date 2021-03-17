# K12 Scraper

Usage (assuming on Linux, macOS, or WSL):

0. Make a temporary directory: `mkdir -p raw-data/`
1. Start virtual environment: `python3 -m venv env`
2. Enable the environment: `source env/bin/activate`
3. Install the prerequisites: `pip install -r requirements.txt`
4. Run: `python scraper.py`

The results are saved to `output.json`. Intermediate, cached HTMLs are saved in `raw-data`.