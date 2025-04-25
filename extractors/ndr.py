import pandas as pd
from bs4 import BeautifulSoup

from extractors.playlist_extractor import PlaylistExtractor


class NdrExtractor(PlaylistExtractor):
    def __init__(self, log = True, sleep_secs = 1):
        super().__init__(log, sleep_secs)
        self.broadcaster = 'ndr'
        self.oldest_timestamp = pd.Timedelta(days=60)
        self.stations = {'ndr1niedersachsen': '1210',
                         'ndr2': '1202',
                         'wellenord': '1204',
                         'radiomv': '1206',
                         '903': '1208',
                         'kultur': '1212',
                         'ndrblue': '1214',
                         'ndrschlager': '1230',
                         'njoy': ''}

    def get_times(self, start, end, station) -> pd.DatetimeIndex:
        return pd.date_range(start, end, freq='1h')

    def get_url(self, station: str, time):
        date = time.strftime('%Y-%m-%d')
        hour = time.strftime('%H')
        if station == 'njoy':
            return f'https://www.n-joy.de/radio/titelsuche118_date-{date}_hour-{hour}.html', {}

        return f'https://www.ndr.de/{station}/programm/titelliste{self.stations[station]}.html?date={date}&hour={hour}', ''

    def extract(self, station: str, document: bytes, date) -> pd.DataFrame:
        log_extra = {'station': station}
        date = date.strftime('%Y-%m-%d')
        soup = BeautifulSoup(document, 'html.parser').find(id='playlist')

        df = pd.DataFrame()
        if soup is None:
            self.logger.warning(f'No playlist data found for {date}', extra=log_extra)
            return df

        if station == 'kultur':
            plural = {'Chöre': 'Chor',
                      'Dirigenten': 'Dirigent',
                      'Ensembles': 'Ensemble',
                      'Solisten': 'Solist'}

            for p in soup.find_all(class_='program'):
                a = [i.text if len(i.find_all()) == 0 else i.find_all()[0].text for i in
                     p.find_all(class_='details_a')]
                a = [plural[i] if i in plural else i for i in a]
                b = [[i.text] if len(i.find_all()) == 0 else ', '.join(e.text for e in i.find_all()) for i in
                     p.find_all(class_='details_b')]
                timestamp = pd.to_datetime(date + ' ' + p.find(class_='time').string)
                if timestamp in df.index:
                    timestamp += pd.Timedelta(seconds=30)

                df = df.combine_first(pd.DataFrame(
                    {'artist': [p.find(class_='artist').string],
                     'title': [p.find(class_='title').string]} | dict(zip(a, b)),
                    index=pd.Series(data=[timestamp], name='time'), dtype=str))

            # to_add.set_index((f - pd.Timedelta(seconds=30)) if f.second == 30 else f for f in list(to_add.index))
        else:
            df = pd.DataFrame({
                'artist': [e.string for e in soup.find_all(class_='artist')],
                'title': [e.string for e in soup.find_all(class_='title')]
            }, index=pd.Series(data=pd.to_datetime([date + ' ' + e.string for e in soup.find_all(class_='time')]),
                               name='time'), dtype=str)

        return df
