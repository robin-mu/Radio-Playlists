import pandas as pd
from bs4 import BeautifulSoup
import re

from extractors.playlist_extractor import PlaylistExtractor


class WdrExtractor(PlaylistExtractor):
    broadcaster = 'wdr'
    oldest_timestamp = pd.Timestamp(2023, 9, 13)
    stations = {'1live': '1live/musik/playlist/index.jsp',
                '1live-diggi': '1live-diggi/onair/1live-diggi-playlist/index.jsp',
                'wdr2': 'wdr2/musik/playlist/index.jsp',
                'wdr3': 'wdr3/titelsuche-wdrdrei-104.jsp',
                'wdr4': 'wdr4/titelsuche-wdrvier-102.jsp',
                'wdr5': 'wdr5/musik/titelsuche-wdrfuenf-104.jsp',
                'cosmo': 'cosmo/musik/playlist/index.jsp'}

    def __init__(self, log=True, sleep_secs=1):
        super().__init__(log, sleep_secs)

    def get_times(self, start, end, station) -> pd.DatetimeIndex:
        return pd.date_range(start, end, freq='1h')

    def get_url(self, station: str, time):
        date = time.strftime('%Y-%m-%d')
        hour = time.strftime('%H')
        form = {'playlistSearch_date': date,
                'playlistSearch_hours': hour,
                'playlistSearch_minutes': "30",
                'submit': 'suchen'}

        return 'https://www1.wdr.de/radio/' + self.stations[station], form

    def extract(self, station: str, document: bytes, date) -> pd.DataFrame:
        log_extra = {'station': station}

        soup = BeautifulSoup(document, 'html.parser').find(id='searchPlaylistResult')

        df = pd.DataFrame()
        if not soup:
            self.logger.warning(f'No playlist data found for {date}', extra=log_extra)
            return df

        df = pd.DataFrame({
            'artist': [e.text.strip() for e in soup.find_all(class_='performer')],
            'title': [e.text.strip() for e in soup.find_all(class_='title')]
        }, index=pd.Series(data=pd.to_datetime([e.text.strip() for e in soup.find_all(class_='datetime')],
                                               format='%d.%m.%Y,%H.%M Uhr'),
                           name='time'), dtype=str)

        if station == 'wdr3':
            df['composer'] = [e.text.strip() for e in soup.find_all(class_='composer')]

            for i, el in enumerate(soup.find_all(class_='performer')):
                delimiters = [e.text for e in el.find_all('strong')]

                if delimiters:
                    pattern = '|'.join(map(re.escape, delimiters))

                    cols = [s.strip(':') for s in delimiters]
                    values = [s.strip().replace('\n', '; ') for s in re.split(pattern, el.text.strip())[1:]]

                    if len(cols) != len(values):
                        self.logger.warning(
                            f'{date}: Length of columns ({len(cols)}) and values ({len(values)}) is not equal',
                            extra=log_extra)

                    df = df.combine_first(
                        pd.DataFrame(dict(zip(cols, values)), index=pd.Series(df.index[i], name='time')))

        return df
