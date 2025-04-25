import pandas as pd
from bs4 import BeautifulSoup

from extractors.playlist_extractor import PlaylistExtractor


class BrExtractor(PlaylistExtractor):
    def __init__(self, log=True, sleep_secs=1):
        super().__init__(log, sleep_secs)
        self.broadcaster = 'br'
        self.oldest_timestamp = pd.Timedelta(days=31)
        self.stations = {'br1': 'radio/bayern-1/welle110',
                         'br2': 'radio/bayern2/welle106',
                         'br3': 'radio/bayern-3/bayern-3-100',
                         'puls': 'puls/welle116',
                         'br-schlager': 'radio/br-schlager/welle118',
                         'br-heimat': 'radio/br-heimat/welle128'}

    def get_times(self, start, end, station) -> pd.DatetimeIndex:
        return pd.date_range(start, end, freq='1h')

    def get_url(self, station: str, time):
        date = time.strftime('%d.%m.%Y')
        hour = time.strftime('%H')
        form = {'date': date,
                'hour': hour}

        return f'https://www.br.de/{self.stations[station]}~playlist.html', form

    def extract(self, station: str, document: bytes, date) -> pd.DataFrame:
        log_extra = {'station': station}

        soup = BeautifulSoup(document, 'html.parser').find(class_='music_research')

        if not soup:
            self.logger.warning(f'No playlist data found for {date}', extra=log_extra)
            return pd.DataFrame()

        time = [date.strftime('%Y%m%d') + ' ' + e.text for e in soup.find_all(class_='time')]

        try:
            artist = [e.find_all('span')[0].text for e in soup.find_all(class_='title')]
            title = [e.find_all('span')[1].text for e in soup.find_all(class_='title')]
        except IndexError:
            self.logger.warning(f'{date}: Title contains less than 2 span elements', extra=log_extra)
            return pd.DataFrame()

        df = pd.DataFrame({
            'artist': artist,
            'title': title
        }, index=pd.Series(data=pd.to_datetime(time, format='%Y%m%d %H:%M'), name='time'),
            dtype=str)

        return df[df['artist'] != '']
