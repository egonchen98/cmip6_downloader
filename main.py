import os
import json
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import tools.download_cmip6 as download
import tools.colab as colab


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    config = json.loads(Path('./resources/config.json').read_text())
    database = config['Database_folder']
    pool = ThreadPoolExecutor(max_workers=5)

    while True:

        records = [colab.get_1_record() for i in range(5)]
        records = list({v['url']: v for v in records}.values())
        if 'Finished' in records:
            break

        # Add downloading threads
        for index, res in enumerate(records):
            pool.submit(colab.run, res, database)
            time.sleep(1)
        pool.shutdown()



