import os

import tools.download_cmip6 as download
import tools.colab as colab


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    job_finished = False
    while not job_finished:
        colab.log_downloaded_files()
        df = colab.get_url_to_download()
        download.run(df)


