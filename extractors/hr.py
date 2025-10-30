import pandas as pd
from bs4 import BeautifulSoup

from extractors.playlist_extractor import PlaylistExtractor


class HrExtractor(PlaylistExtractor):
    broadcaster = 'hr'
    oldest_timestamp = pd.Timedelta(days=14)
    stations = {'hr1': 'https://www.hr1.de/titelliste/playlist_hrone-100~inline_date-%s_hour-%s.html',
                'hr2-kultur': 'https://www.hr2.de/hrzwei-playlist-100~inline_date-%s_hour-%s.html',
                'hr3': 'https://www.hr3.de/playlist/playlist_hrthree-100~inline_date-%s_hour-%s.html',
                'hr4': 'https://www.hr4.de/musik/titelliste/playlist_hrfour-100~inline_date-%s_hour-%s.html',
                'youfm': 'https://www.you-fm.de/playlists/was-lief-wann/playlist_you-fm-100~inline_date-%s_hour-%s.html'}

    def __init__(self, log=True, sleep_secs=1):
        super().__init__(log, sleep_secs)

    def get_times(self, start, end, station) -> pd.DatetimeIndex:
        return pd.date_range(start, end, freq='1h')

    def get_url(self, station: str, time):
        date = time.strftime('%Y-%m-%d')
        hour = time.strftime('%H')

        return self.stations[station] % (date, hour), {}

    def extract(self, station: str, document: bytes, date) -> pd.DataFrame:
        log_extra = {'station': station}

        soup = BeautifulSoup(document, 'html.parser')

        df = pd.DataFrame()
        if (not soup.find_all(class_='text__headline') or
                soup.find_all(class_='text__headline')[
                    0].string.strip() == 'Es liegen derzeit keine Playlistdaten vor.'):
            self.logger.warning(f'No playlist data found for {date}', extra=log_extra)
            return df

        df = pd.DataFrame({
            'artist': [e.find_all()[0].string.strip() for e in soup.find_all(itemprop='byArtist')],
            'title': [e.string.strip() for e in soup.find_all(class_='text__headline')],
            'duration': [float(e['content'][1:-1]) for e in soup.find_all('time')]
        }, index=pd.Series(
            data=pd.to_datetime([pd.to_datetime(e['datetime']).tz_localize(None) for e in soup.find_all('time')]),
            name='time'), dtype=str)

        if station == 'hr2-kultur':
            composers = []
            for li in soup.find_all('li'):
                c = li.find(itemprop='composer')
                if c:
                    composers.append(c.string.strip())
                else:
                    composers.append('')

            if len(composers) != df.shape[0]:
                self.logger.warning(f'{date}: Length of composers ({len(composers)}) and data ({df.size}) is not equal',
                                    extra=log_extra)

            df['composer'] = composers

        return df
