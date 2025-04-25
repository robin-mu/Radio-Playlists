import ctypes
import json
import logging.config
import os
import sys
import glob
import time
from timeit import default_timer as timer
from typing import Any

import pandas as pd
import requests
from requests import Response
from tqdm.auto import tqdm

class PlaylistExtractor:
    logging_config_loaded = False

    def __init__(self, log: bool = True, sleep_secs: int = 1):
        self.broadcaster: str = ''
        self.stations: list[str] | dict[str, Any] = {}
        self.oldest_timestamp: pd.Timedelta | pd.Timestamp | dict[str, pd.Timedelta | pd.Timestamp] = pd.Timestamp.now()
        self.sleep_secs: int = sleep_secs
        self.file_extension: str = 'html'

        if not os.path.isdir('logs'):
            os.mkdir('logs')
        if not os.path.isdir('data'):
            os.mkdir('data')
        if not os.path.isdir('raw'):
            os.mkdir('raw')

        if not PlaylistExtractor.logging_config_loaded:
            with open('logging_config.json', 'r') as f:
                config = json.load(f)
            logging.config.dictConfig(config)
            PlaylistExtractor.logging_config_loaded = True

        self.logger = logging.getLogger('RadioPlaylists')
        self.logger.setLevel(logging.DEBUG)

        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                                                   'Chrome/112.0.0.0 '
                                                   'Safari/537.36 OPR/98.0.0.0'})

    def get_times(self, start: pd.Timestamp, end: pd.Timestamp, station: str) -> pd.DatetimeIndex:
        """Generates all timestamps necessary to request data between start and end"""
        pass

    def get_url(self, station: str, time: pd.Timestamp) -> tuple[str, str]:
        """Returns the url to access the playlist data and the form data if a POST is used"""
        pass

    def extract(self, station: str, document: bytes, time) -> pd.DataFrame:
        """Extracts the playlist information from the downloaded document and puts it into a DataFrame"""
        pass

    def download(self, station: str, start, end, progress_bar=None) -> pd.DataFrame:
        def try_post() -> Response | None:
            while True:
                try:
                    url, data = self.get_url(station, t)
                    if data:
                        req = self.session.post(url, data)
                    else:
                        req = self.session.get(url)
                except requests.exceptions.RequestException as e:
                    self.logger.warning(f'Error while downloading data from {t}: {e} (trying again)', extra=log_extra)
                    time.sleep(self.sleep_secs)
                    continue

                if req.status_code != 200:
                    self.logger.warning(
                        f'Bad status code while downloading data from {t}: {req.status_code} {req.reason}',
                        extra=log_extra)

                time.sleep(self.sleep_secs)
                return req

        ctypes.windll.kernel32.SetThreadExecutionState(0x80000001)

        log_extra = {'station': station}

        progress_bar = progress_bar or tqdm(desc=f'{self.broadcaster}: {station}', file=sys.stdout,
                                            total=(end - start) // pd.Timedelta(minutes=1), unit='h', unit_scale=60, leave=False,
                                            bar_format="{desc:<20.20}{percentage:3.0f}%|{bar:40}{r_bar}")

        # Downloading
        new_files: list[str] = []

        present_files = glob.glob(f'raw/{self.broadcaster}_{station}_*')
        if present_files:
            newest_date = pd.to_datetime(max(present_files).split('_')[-1].split('.')[0], format='%Y%m%d-%H%M%S').floor('1d') - pd.Timedelta(days=1)
        else:
            newest_date = pd.Timestamp.now()

        prev_t = None
        for t in self.get_times(start, end, station):
            if prev_t is None:
                prev_t = t

            filepath = os.path.join('raw', f'{self.broadcaster}_{station}_{t.strftime("%Y%m%d-%H%M%S")}.{self.file_extension}')
            if os.path.isfile(filepath) and t < newest_date:
                status_msg = f'File for {t} is already present at {filepath}'
            else:
                request_timer = timer()
                req = try_post()
                with open(filepath, 'wb') as f:
                    f.write(req.content)

                status_msg = f'Downloaded data from {t} ({timer() - request_timer - self.sleep_secs:.3f}s)'
                progress_bar.set_postfix_str(status_msg)

            new_files.append(filepath)
            self.logger.info(status_msg, extra=log_extra)

            try:
                progress_bar.update(abs(t - prev_t) // pd.Timedelta(minutes=1))
            except TypeError as e:  # if n > total, tqdm will throw a TypeError
                self.logger.error(f"Exception while updating the progress bar: {e}", extra=log_extra)
                progress_bar.total = progress_bar.n
                progress_bar.refresh()

            prev_t = t

        # Extracting
        pages = []
        for path in new_files:
            date = pd.to_datetime(path.split('_')[-1].split('.')[0], format='%Y%m%d-%H%M%S')
            with open(path, 'rb') as f:
                file = f.read()

            extracted = self.extract(station, file, date)
            pages.append(extracted)

            status_msg = f'Extracted data from {path} - {len(extracted)} elements found'
            progress_bar.set_postfix_str(status_msg)
            self.logger.info(status_msg, extra=log_extra)

        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)

        if not pages:
            return pd.DataFrame()

        return pd.concat(pages)

    def update_databases(self, stations: list[str] | None=None):
        stations = stations or self.stations
        with tqdm(file=sys.stdout, leave=False, unit='h', unit_scale=1/60,
                  bar_format='{desc:<30.30}{percentage:3.0f}%|{bar:40}{r_bar}') as pbar:
            time_ranges = {}  # precalculate start and end time for each station for correct progress bar
            for station in stations:
                path = os.path.join('data', f'{self.broadcaster}_{station}.csv')

                if os.path.isfile(path):
                    df = pd.read_csv(path, parse_dates=[0], index_col='time')
                else:
                    df = pd.DataFrame()

                start = self.oldest_timestamp[station] if isinstance(self.oldest_timestamp,
                                                                     dict) else self.oldest_timestamp

                if isinstance(start, pd.Timedelta):
                    start = (pd.Timestamp.now() - start)

                if not df.empty:
                    start = max(start, df.iloc[-1].name)

                start = start.floor('1d')
                end = pd.Timestamp.now().ceil('1d')

                if start > end:
                    raise ValueError(f'{station}: End time is later than start time')

                time_ranges[station] = (start, end)

            pbar.total = sum((end - start) // pd.Timedelta(minutes=1) for start, end in time_ranges.values())
            pbar.bar_format = '{desc:<30.30}{percentage:3.0f}%|{bar:40}| {n:.0f}/{total:.0f} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'

            for station in stations:
                path = os.path.join('data', f'{self.broadcaster}_{station}.csv')

                if os.path.isfile(path):
                    df = pd.read_csv(path, parse_dates=[0], index_col='time')
                else:
                    df = pd.DataFrame()

                start, end = time_ranges[station]

                try:
                    pbar.set_description_str(f'{self.broadcaster}: {station}')
                    pbar.set_postfix_str(f'Downloading data between {start} and {end}')
                except TypeError as e:  # if n > total, tqdm will throw a TypeError
                    self.logger.error(f"Exception while updating the progress bar: {e}", extra={'station': station})
                    pbar.total = pbar.n
                    pbar.refresh()

                new_data = self.download(station, start, end, progress_bar=pbar)

                df = pd.concat([df, new_data])
                df.index.rename('time', inplace=True)
                df['time'] = df.index
                df.drop_duplicates(inplace=True)
                df.drop(columns='time', inplace=True)
                df.sort_index().to_csv(path)