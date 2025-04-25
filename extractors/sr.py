import pandas as pd
from bs4 import BeautifulSoup

from extractors.playlist_extractor import PlaylistExtractor


class SrExtractor(PlaylistExtractor):
    def __init__(self, log=True, sleep_secs=1):
        super().__init__(log, sleep_secs)
        self.broadcaster = 'sr'
        self.oldest_timestamp = {'sr1': pd.Timedelta(days=3),
                                 'sr2': pd.Timedelta(days=13),
                                 'sr3': pd.Timedelta(days=3)}
        self.stations = ['sr1', 'sr2', 'sr3']

    def get_times(self, start, end, station) -> pd.DatetimeIndex:
        return pd.date_range(start, end, freq='1h')

    def get_url(self, station: str, time):
        date_offset = (pd.Timestamp.now().floor('1d') - time.floor('1d')).days
        hour = time.hour

        return f'https://musikrecherche.sr.de/{station}/musicresearch.php?DateSelect={date_offset}&TimeSelect={hour}', {}

    def extract(self, station: str, document: bytes, date) -> pd.DataFrame:
        log_extra = {'station': station}

        soup = BeautifulSoup(document, 'html.parser').find(class_='musicResearch')

        if not soup:
            self.logger.warning(f'No playlist data found for {date}', extra=log_extra)
            return pd.DataFrame()

        if station != 'sr2':
            time = [date.strftime('%Y%m%d') + ' ' + e.text for e in soup.find_all(class_='musicResearch__Item__Time')]
            content = soup.find_all(class_='musicResearch__Item__Content')
            artist = [e.find(class_='musicResearch__Item__Content__Artist').text for e in content]
            title = [e.find(class_='musicResearch__Item__Content__Title').text for e in content]

            df = pd.DataFrame({
                'artist': artist,
                'title': title
            }, index=pd.Series(data=pd.to_datetime(time, format='%Y%m%d %H:%M'), name='time'),
               dtype=str)
        else:
            df = pd.DataFrame()
            for time, content in zip(soup.find_all(class_='musicResearch__Item__Time'),
                                     soup.find_all(class_='musicResearch__Item__Content')):
                if content.find(class_='background-title'):
                    continue

                titles = content.find_all(class_='musicResearch__Item__Content__Title')
                if len(titles) == 1:
                    composer = ''
                    title = titles[0].text
                elif len(titles) == 2:
                    composer = titles[0].text
                    title = titles[1].text
                else:
                    self.logger.warning(f'More than 2 composers for {date}', extra=log_extra)
                    composer = ''
                    title = ''

                artists = []
                for artist in content.find_all(class_='musicResearch__Item__Content__Artist'):
                    if ':' in artist.text:
                        break
                    artists.append(artist.text.strip())

                date = pd.to_datetime(date.strftime('%Y%m%d') + ' ' + time.text, format='%Y%m%d %H:%M')
                df = pd.concat([df, pd.DataFrame({'composer': composer, 'title': title, 'artist': '; '.join(artists)},
                                                 index=pd.Series(date, name='time'))])

        return df
