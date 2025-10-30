from concurrent.futures import ThreadPoolExecutor
from wakepy import keep

from extractors import *
from extractors.playlist_extractor import PlaylistExtractor

extractors = [a for a in globals().values() if isclass(a) and issubclass(a, PlaylistExtractor) and a != PlaylistExtractor]

with keep.running():
    with ThreadPoolExecutor() as ex:
        future_list = []
        for cls in extractors:
            future_list.append(ex.submit(cls().update_databases))

        for future in future_list:
            future.result()
