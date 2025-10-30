import pandas as pd
from bs4 import BeautifulSoup

from extractors.playlist_extractor import PlaylistExtractor


class NdrExtractor(PlaylistExtractor):
    broadcaster = 'ndr'
    oldest_timestamp = pd.Timedelta(days=60)
    stations = {'ndr1niedersachsen': 'titelliste-ndr-1-niedersachsen,radioplaylist-ndr1niedersachsen-100',
                'ndr2': 'ndr2-playlist,radioplaylist-ndr2-100',
                'wellenord': 'titelliste-ndr-1-welle-nord,radioplaylist-wellennord-100',
                'radiomv': 'titelliste-ndr-1-radio-mv,radioplaylist-radiomv-100',
                '903': 'titelliste-ndr-903,radioplaylist-neunzigdrei-100',
                'kultur': 'titelliste-ndr-kultur,ndrkultur-titelliste-100',
                'ndrblue': 'ndr-blue-titelliste,radioplaylist-ndrblue-100',
                'ndrschlager': 'titelliste-ndr-schlager,radioplaylist-ndrschlager-100',
                'n-joy': ''}

    def __init__(self, log=True, sleep_secs=1):
        super().__init__(log, sleep_secs)

    def get_times(self, start, end, station) -> pd.DatetimeIndex:
        return pd.date_range(start, end, freq='1h')

    def get_url(self, station: str, time):
        date = time.strftime('%Y-%m-%d')
        hour = time.strftime('%H')
        if station == 'n-joy':
            return f'https://www.ndr.de/n-joy/musik/n-joy-playlist,radioplaylist-njoy-100.html?date={date}&hour={hour}', ''

        return f'https://www.ndr.de/{station}/programm/{self.stations[station]}.html?date={date}&hour={hour}', ''

    def extract(self, station: str, document: bytes, date) -> pd.DataFrame:
        log_extra = {'station': station}
        date = date.strftime('%Y-%m-%d')
        soup = BeautifulSoup(document, 'html.parser').find(id='titlelist')

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
                keys = [i.text if len(i.find_all()) == 0 else i.find_all()[0].text for i in
                        p.find_all(class_='additionalinfo--key')]
                keys = [plural[i] if i in plural else i for i in keys]
                values = [[i.text] if len(i.find_all()) == 0 else ', '.join(e.text for e in i.find_all()) for i in
                          p.find_all(class_='additionalinfo--value')]
                timestamp: pd.Timestamp = pd.to_datetime(date + ' ' + p.find(class_='timeandplay').string,
                                           format='%Y-%m-%d %H:%M Uhr')
                if timestamp in df.index:
                    timestamp += pd.Timedelta(seconds=30)

                df = df.combine_first(pd.DataFrame(
                    {'artist': [p.find(class_='artist').string],
                     'title': [p.find(class_='title').string]} | dict(zip(keys, values)),
                    index=pd.Series(data=[timestamp], name='time'), dtype=str))

            # to_add.set_index((f - pd.Timedelta(seconds=30)) if f.second == 30 else f for f in list(to_add.index))
        else:
            df = pd.DataFrame({
                'artist': [e.string for e in soup.find_all(class_='artist')],
                'title': [e.string for e in soup.find_all(class_='title')]
            }, index=pd.Series(data=pd.to_datetime([date + ' ' + e.string for e in soup.find_all(class_='timeandplay')],
                                                   format='%Y-%m-%d %H:%M Uhr'),
                               name='time'), dtype=str)

        return df
