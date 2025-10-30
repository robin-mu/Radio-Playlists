# Radio Playlists
A library for extracting playlist data (which song was played at which time) for various radio stations.

# Usage
Execute `update_all.py` to create databases for all radio broadcasters which have a class in the `extractors` folder.  
To create a database for all stations of a single broadcaster, import and instantiate the class corresponding to the broadcaster and run the `update_databases` method. You can optionally specify which stations should be downloaded, passing no arguments will download all stations.  
Raw data (html, json etc., depending on the infrastructure of the broadcaster) whill be saved in the `raw` folder, which can be emptied after the script finished. The database is a csv file located at `data/{broadcaster}_{station}.csv` containing the columns time, artist, title and optionally more metadata.

# Contributing
If you want to add a broadcaster, you need to [fork](https://github.com/robin-mu/Radio-Playlists/fork) this repository, create a Python file in the `extractors` folder containing a class which inherits from the `PlaylistExtractor` class located in `extractors/playlist_extractor.py`. Your class has to call `super().__init__()` in its `__init__` method. It has to define the following class attributes:
- `broadcaster`: The name of the broadcaster as a string
- `stations`: List of station names this broadcaster manages, or dictionary with station names as keys and anything you need for extraction (e.g. urls) as values
- `oldest_timestamp`: The oldest time for which playlist data is accessible. This can be a 
  - `pd.Timestamp` if a broadcaster saves playlist data starting from a fixed date
  - `pd.Timedelta` if a broadcaster deletes playlist data older than some timedelta (e.g. `self.oldest_timestamp = pd.Timedelta(days=14)` if the broadcaster deletes playlist data older than two weeks)
  - Dictionary with station names as keys and `pd.Timestamp` or `pd.Timedelta` as values if the oldest time is different for each station
- If this broadcaster uses a different file extension than html (e.g. json), you can optionally define a `file_extension` attribute

Your class has to implement the following methods:
- `get_times(self, start: pd.Timestamp, end: pd.Timestamp, station: str) -> Iterable[pd.Timestamp]`: Can return a DatetimeIndex containing all timestamps necessary to request data from a given station between a given start and end time, e.g. `return pd.date_range(start, end, freq='1h')` if the broadcaster provides one hour of playlist content per request.  
This method can also be a generator which calculates and yields the next timestamp when requested, which is useful if the timestamp of the next request depends on the result of the previous request (e.g. the broadcaster provides a constant number of playlist entries per request)
- `get_url(self, station: str, time: pd.Timestamp) -> tuple[str, str]`: The first element of the returned tuple is the url to access the playlist data from the given station at the given timestamp. If a POST request is used, the second tuple element contains the form data
- `extract(self, station: str, document: bytes, time) -> pd.DataFrame`: Extracts the playlist information from the downloaded document (html, json, etc.) and puts it into a DataFrame. The index of the DataFrame has to be the timestamp for each song. 

For logging, you can use the logger object `self.logger` which is defined in the `PlaylistExtractor` base class. If the name of the station should show up in log messages, you have to add `log_extra={'station': '{your_station}'}` for each logging call.

# Dependencies
- requests: For HTTP requests
- beautifulsoup4: For extracting playlist information from html files
- pandas: For managing playlist databases
- tqdm: For pretty progress bars
- wakepy: To keep the system awake while updating (OS independent)