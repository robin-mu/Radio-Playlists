import pandas as pd
from bs4 import BeautifulSoup

from extractors.playlist_extractor import PlaylistExtractor


class SwrExtractor(PlaylistExtractor):
    def __init__(self, log=True, sleep_secs=1):
        super().__init__(log, sleep_secs)
        self.broadcaster = 'swr'
        self.oldest_timestamp = pd.Timedelta(days=90)
        self.stations = {'swr1': 'https://www.swr.de/swr1/bw/playlist/index.html',
                         'swr3': 'https://www.swr3.de/playlisten/index.html',
                         'swr4': 'https://www.swr.de/swr4/musik/musikrecherche-s-bw-102.html',
                         'dasding': 'https://www.dasding.de/03-playlistsuche/index.html'}

    def get_times(self, start, end, station) -> pd.DatetimeIndex:
        return pd.date_range(start, end, freq='10min') if station == 'dasding' else pd.date_range(start, end, freq='1h')

    def get_url(self, station: str, time):
        date = time.strftime('%Y-%m-%d')
        time = time.strftime('%H%%3A%M')

        return self.stations[station] + f'?swx_date={date}&swx_time={time}&_pjax=%23content', {}

    def extract(self, station: str, document: bytes, date) -> pd.DataFrame:
        log_extra = {'station': station}

        soup = BeautifulSoup(document, 'html.parser').find(class_='list-playlist')

        if not soup:
            self.logger.warning(f'No playlist data found for {date}', extra=log_extra)
            return pd.DataFrame()

        df = pd.DataFrame({
            'artist': [e.text.strip() for e in soup.find_all('dd', class_='playlist-item-song')],
            'title': [e.text.strip() for e in soup.find_all('dd', class_='playlist-item-artist')]
        }, index=pd.Series(data=pd.to_datetime([e['datetime'] for e in soup.find_all('time')], format='%Y-%m-%dT%H:%M'), name='time'),
           dtype=str)

        return df
