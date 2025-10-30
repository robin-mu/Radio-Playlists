import time

import pandas as pd
from bs4 import BeautifulSoup
from six import StringIO

from extractors.playlist_extractor import PlaylistExtractor


class RbbExtractor(PlaylistExtractor):
    broadcaster = 'rbb'
    oldest_timestamp = {'888': pd.Timestamp(2025, 5, 1),
                        'antenne-brandenburg': pd.Timestamp(2025, 5, 1),
                        'fritz': pd.Timestamp(2019, 1, 1),
                        'radioeins': pd.Timestamp(2022, 3, 21),
                        'radiodrei': pd.Timestamp(2023, 1, 1)}
    stations = {'888': 'rbb888',
                'antenne-brandenburg': 'antenne_brandenburg',
                'fritz': 'https://www.fritz.de/programm/sendungen/playlists/index.htm/',
                'radioeins': 'https://www.radioeins.de/musik/playlists.htm/',
                'radiodrei': 'https://www.radiodrei.de/musik/musiklisten/index.htm/'}

    def __init__(self, log=True, sleep_secs=1):
        super().__init__(log, sleep_secs)
        self.times = None

    def get_times(self, start, end, station) -> pd.DatetimeIndex:
        log_extra = {'station': station}

        if station in ['888', 'antenne-brandenburg']:
            return pd.date_range(start, end, freq='1h')

        self.times = {}
        while True:
            start_str = start.strftime('%d-%m-%Y_%H-%M')
            end_str = end.strftime('%d-%m-%Y_%H-%M')
            url = self.stations[station] + f'from={start_str}/module=playlistfinder/to={end_str}.html'

            self.logger.info(f'get_times: Downloading {url}', extra=log_extra)
            soup = BeautifulSoup(self.session.get(url).content, 'html.parser')

            urls = [f'https://www.{station}.de{e.find("a")["href"]}' for e in
                    soup.find_all(class_='play_time' if station == 'radioeins' else 'begin')]
            if not urls:
                break

            times = list(
                pd.to_datetime([e.split('/')[-1].split('.')[0].ljust(13, '0') for e in urls], format='%y%m%d_%H%M%S'))
            seen = set()
            for i, e in enumerate(times):
                if e in seen:
                    while e in times or e in self.times:
                        e = e.replace(second=e.second + 1)
                    times[i] = e
                else:
                    seen.add(e)

            self.times.update(dict(zip(times, urls)))

            end = sorted(times)[0]
            time.sleep(self.sleep_secs)

        return pd.DatetimeIndex(sorted(self.times.keys()))

    def get_url(self, station: str, time):
        if station in ['888', 'antenne-brandenburg']:
            date = time.strftime('%Y-%m-%d')
            hour = time.strftime('%H:%M:%S')
            form = {'datum': date,
                    'zeit': hour}

            return f'https://playlisten.rbb-online.de/{self.stations[station]}/main/anzeige.php', form

        return self.times[time], {}

    def extract(self, station: str, document: bytes, date) -> pd.DataFrame:
        log_extra = {'station': station}

        if station in ['888', 'antenne-brandenburg']:
            df = pd.read_html(document)[0]

            if df.loc[
                0, 'Datum'] == 'Es liegen uns für den gew%auml;hlten Zeitraum keine Einträge vor. Bitte verändern Sie Ihre Suchanfrage.':
                self.logger.warning(f'No playlist data found for {date}', extra=log_extra)
                return pd.DataFrame()

            df.index = pd.to_datetime(df.pop('Datum') + ' ' + df.pop('Zeit'), format='%d.%m.%Y %H:%M')
            df.rename(columns={'Titel': 'title', 'Interpret': 'artist'}, inplace=True)
            return df

        soup = BeautifulSoup(document, 'html.parser').find(class_='playlist_tables')
        playlist = soup.find(class_='playlist_aktueller_tag')
        if not playlist:
            self.logger.warning(f'No playlist data found for {date}', extra=log_extra)
            return pd.DataFrame()

        if station == 'fritz':
            df = pd.read_html(StringIO(str(playlist)))[0][['Zeit', 'Künstler.1', 'Titel']]
            if all(df['Künstler.1'].isna()):
                self.logger.warning(f'No artists found for {date}', extra=log_extra)

            playlist_startstop = soup.find(class_='playlisttime').text.split(' - ')
            time = pd.to_datetime(playlist_startstop[0], format='%H:%M')
            if len(playlist_startstop) > 1:
                time = time.replace(second=int(playlist_startstop[1][:2]))
            else:
                self.logger.warning(f'No end time found for {date}', extra=log_extra)
                time = time.replace(second=time.hour + 1)
            if time.second == 0:
                time = time.replace(second=24)

            times = pd.to_datetime(df.pop('Zeit'), format='%H:%M').fillna(time)
            df.index = pd.to_datetime(date.strftime('%Y%m%d') + ' ' + times.dt.strftime('%H%M%S'),
                                      format='%Y%m%d %H%M%S')
            df.rename(columns={'Titel': 'title', 'Künstler.1': 'artist'}, inplace=True)
        elif station == 'radioeins':
            playlist_startstop = soup.find(class_='playlisttime').text.split(' - ')
            playlist_time = pd.to_datetime(playlist_startstop[0], format='%H:%M')
            if len(playlist_startstop) > 1:
                playlist_time = playlist_time.replace(second=int(playlist_startstop[1][:2]))
            else:
                self.logger.warning(f'No end time found for {date}', extra=log_extra)
                playlist_time = playlist_time.replace(second=playlist_time.hour + 1)
            if playlist_time.second == 0:
                playlist_time = playlist_time.replace(second=24)

            times = []
            titles = []
            artists = []
            albums = []

            for row in playlist.find_all('tr'):
                if 'play_track' in row['class']:
                    play_time = pd.to_datetime(row.find(class_='play_time').text, format='%H:%M')
                    times.append(date.strftime('%Y%m%d') + ' ' + (
                        play_time.strftime('%H%M%S') if pd.notna(play_time) else playlist_time.strftime('%H%M%S')))

                    title = row.find('span', class_='tracktitle')
                    titles.append(title.text if title else '')
                    artist = row.find('span', class_='trackinterpret')
                    artists.append(artist.text if artist else '')
                    album = row.find('span', class_='trackalbum')
                    albums.append(album.text if album else '')

            df = pd.DataFrame({
                'artist': artists,
                'title': titles,
                'album': albums
            }, index=pd.Series(data=pd.to_datetime(times, format='%Y%m%d %H%M%S'), name='time'),
                dtype=str)
        else:
            playlist_startstop = soup.find(class_='playlisttime').text.split(' - ')
            time = pd.to_datetime(playlist_startstop[0], format='%H:%M')
            if len(playlist_startstop) > 1:
                time = time.replace(second=int(playlist_startstop[1][:2]))
            else:
                self.logger.warning(f'No end time found for {date}', extra=log_extra)
                time = time.replace(second=time.hour + 1)
            if time.second == 0:
                time = time.replace(second=24)

            times = []
            composers = []
            titles = []
            artists = []
            albums = []
            durations = []

            for row in playlist.find_all('tr'):
                if 'fond' in row['class']:
                    try:
                        time = pd.to_datetime(row.find(class_='play_time').text.split(' ')[0], format='%H:%M')
                    except ValueError:
                        self.logger.warning(f'No time found in fond row for {date}', extra=log_extra)
                elif 'play_track' in row['class']:
                    times.append(date.strftime('%Y%m%d') + ' ' + time.strftime('%H:%M'))

                    composer = row.find('span', class_='trackkomponist')
                    composers.append(composer.text if composer else '')
                    title = row.find('span', class_='tracktitle')
                    titles.append(title.text if title else '')
                    artist = row.find('span', class_='trackinterpret')
                    artists.append(artist.text if artist else '')
                    album = row.find('span', class_='trackalbum')
                    albums.append(album.text if album else '')
                    duration = row.find(class_='tracklength')
                    durations.append(duration.text if duration else '')

            df = pd.DataFrame({
                'artist': artists,
                'title': titles,
                'composer': composers,
                'album': albums,
                'duration': durations
            }, index=pd.Series(data=pd.to_datetime(times, format='%Y%m%d %H:%M'), name='time'),
                dtype=str)

        return df
