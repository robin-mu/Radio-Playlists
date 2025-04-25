import pandas as pd
import os

from extractors.playlist_extractor import PlaylistExtractor


class RadiobremenExtractor(PlaylistExtractor):
    def __init__(self, log=True, sleep_secs=1):
        super().__init__(log, sleep_secs)
        self.broadcaster = 'radiobremen'
        self.oldest_timestamp = pd.Timestamp(2023, 6, 24)
        self.stations = {'bremeneins': 'https://www.bremeneins.de/suche/titelsuche-110~ajax.html',
                         'bremenzwei': 'https://www.bremenzwei.de/musik/titelsuche-106~ajax.html',
                         'bremenvier': 'https://www.bremenvier.de/titelsuche-102~ajax.html',
                         'bremennext': 'https://www.bremennext.de/suche/titelsuche-118~ajax.html'}

        self.last_timestamp = None

    def get_times(self, start, end, station) -> pd.DatetimeIndex:
        self.last_timestamp = end
        while self.last_timestamp > start:
            yield self.last_timestamp

            filepath = os.path.join('raw', f'{self.broadcaster}_{station}_{self.last_timestamp.strftime("%Y%m%d-%H%M%S")}.{self.file_extension}')
            try:
                songs = pd.read_html(filepath)[0]
                last_entry_datetime = pd.to_datetime(self.last_timestamp.strftime('%Y%m%d') + ' ' + songs.iloc[-1]['Uhrzeit'], format='%Y%m%d %H:%M')
                request_time = self.last_timestamp.time()
                last_entry_time = last_entry_datetime.time()

                if last_entry_time.hour - request_time.hour > 12:  # rollover to previous day
                    self.last_timestamp = last_entry_datetime - pd.Timedelta(days=1)
                elif last_entry_time >= request_time:  # no songs in previous hour
                    self.last_timestamp -= pd.Timedelta(hours=1)
                elif request_time.hour - last_entry_time.hour > 12:  # no songs in previous hour, and the next song is on a later day
                    self.last_timestamp -= pd.Timedelta(hours=1)
                else:
                    self.last_timestamp = last_entry_datetime

            except ValueError:
                self.last_timestamp -= pd.Timedelta(hours=1)


    def get_url(self, station: str, time):
        date = time.strftime('%Y-%m-%d')
        hour_minute = time.strftime('%H:%M')

        return f'{self.stations[station]}?playlistsearch-searchDate={date}&playlistsearch-searchTime={hour_minute}', {}

    def extract(self, station: str, document: bytes, date) -> pd.DataFrame:
        log_extra = {'station': station}

        try:
            df = pd.read_html(document)[0]
            df.rename(columns={'Uhrzeit': 'time', 'Interpret': 'artist', 'Titel': 'title'}, inplace=True)
            df['time'] = pd.to_datetime(date.strftime('%Y%m%d') + ' ' + df['time'], format='%Y%m%d %H:%M')

            request_time = date.time()
            df['time'] = df['time'].apply(lambda time: time if time.time() <= request_time else time - pd.Timedelta(days=1))
            df.set_index('time', inplace=True)

            return df
        except ValueError:
            self.logger.warning(f'No playlist data found for {date}', extra=log_extra)
            return pd.DataFrame()