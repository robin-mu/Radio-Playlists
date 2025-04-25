import pandas as pd
import json
import os

from extractors.playlist_extractor import PlaylistExtractor


class MdrExtractor(PlaylistExtractor):
    def __init__(self, log=True, sleep_secs=1):
        super().__init__(log, sleep_secs)
        self.broadcaster = 'mdr'
        self.oldest_timestamp = pd.Timedelta(days=366)
        self.file_extension = 'json'
        self.stations = {'jump': 1,
                         'aktuell': 2,
                         'sputnik': 3,
                         'sachsen': 4,
                         'sachsen-anhalt': 5,
                         'thueringen': 6,
                         'klassik': 7,
                         'kultur': 8,
                         'schlagerwelt': 22,
                         'tweens': 23}

        self.last_timestamp = None

    def get_times(self, start, end, station) -> pd.DatetimeIndex:
        self.last_timestamp = end
        while self.last_timestamp > start:
            yield self.last_timestamp

            filepath = os.path.join('raw', f'{self.broadcaster}_{station}_{self.last_timestamp.strftime("%Y%m%d-%H%M%S")}.{self.file_extension}')
            with open(filepath) as f:
                songs = json.load(f)['Songs']

            if not songs:
                self.last_timestamp -= pd.Timedelta(days=1)
            else:
                self.last_timestamp = pd.to_datetime(songs[list(songs.keys())[-1]]['starttime'], format='%Y-%m-%d %H:%M:%S')

    def get_url(self, station: str, time):
        date = time.strftime('%Y%m%d%H%M%S')
        start = (pd.Timestamp.now() - self.oldest_timestamp).strftime('%Y%m%d')

        return f'https://www.mdr.de/scripts4/titellisten/xmlresp-index.do?output=json&idwelle={self.stations[station]}&amount=1000&startdate={start}&stopdate={date}', {}

    def extract(self, station: str, document: bytes, date) -> pd.DataFrame:
        log_extra = {'station': station}

        data = json.loads(document)['Songs']

        df = pd.DataFrame()
        if not data:
            self.logger.warning(f'No playlist data found for {date}', extra=log_extra)
            return df

        df = pd.DataFrame.from_dict(data, orient='index')
        df.set_index(pd.DatetimeIndex(pd.to_datetime(df['starttime']), name='time'), inplace=True)

        drop_columns = ['status', 'id_titel', 'av_next_id', 'starttime', 'artist_image_id', 'transmissiontype',
                        'audioasset']
        df.drop(drop_columns, axis=1, inplace=True)

        df['duration'] = df['duration'].apply(lambda x: pd.to_timedelta(x).seconds)

        self.last_timestamp = df.index[-1]

        return df
