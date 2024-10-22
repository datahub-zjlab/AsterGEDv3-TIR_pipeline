import os
import logging
import requests
from config_save import token, src_bucket

# Set logs
logging.basicConfig(filename='tir_pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

filelist_txt = 'AG100_filelist.txt'
file_list = []
with open(filelist_txt, 'r', encoding='utf-8') as file:
    for line in file:
        file_list.append(line.strip())


def download_file(base_url, file_name, download_path, token):
    '''
    :param base_url: Download page
    :param file_name: Filename
    :param download_path: Save folder of the files
    :param token: Granted the download permission by the registered account
    '''
    url = f"{base_url}{file_name}"
    file_path = os.path.join(download_path, file_name)

    # Check if the file already exists
    if os.path.exists(file_path):
        logging.info(f"File {file_name} already exists. Skipping download.")
        return

    # Use requests to download the file
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, stream=True, headers=headers)
        response.raise_for_status()  # Ensure the request was successful

        # Get the total size of the file
        total_size = int(response.headers.get('content-length', 0))

        # Write the file with progress bar
        with open(file_path, 'wb') as file:
            for data in response.iter_content(chunk_size=1024):
                size = file.write(data)

        # Check if the file is complete
        if os.path.getsize(file_path) != total_size:
            logging.info(f"Download of {file_name} is incomplete. Deleting file.")
            os.remove(file_path)
        print(f"{file_name} download ok")

    except requests.exceptions.RequestException as e:
        logging.info(f'Error downloading {url}: {e}')
        if os.path.exists(file_path):
            os.remove(file_path)  # Remove the partially downloaded file
    except TimeoutError as e:
        logging.info(f'Timeout downloading {url}: {e}')
        if os.path.exists(file_path):
            os.remove(file_path)  # Remove the partially downloaded file


def count_files_with_suffix(bucket, prefix, file_suffix='h5'):
    '''
    # Check whether the downloaded files in oss are complete
    :param bucket: oss bucket
    :param prefix: oss bucket prefix
    :param file_suffix: find the files with fixed suffix
    '''
    fn_list = []

    marker = None
    while True:
        result = bucket.list_objects(prefix=prefix, marker=marker)

        a = result.object_list
        for item in a:
            if item.key.split('.')[-1] == file_suffix:
                fn_list.append(item.key)

        # for obj in result.object_list:
        #     print('Found file:', obj.key)

        if not result.is_truncated:
            break

        marker = result.next_marker

    return fn_list


# AsterGEDv3 Download page: "https://e4ftl01.cr.usgs.gov/ASTT/AG100.003/2000.01.01/"
aster_url = "https://e4ftl01.cr.usgs.gov/ASTT/AG100.003/2000.01.01/"
download_file(aster_url, file_name="AG100.v003.-30.-082.0001.h5", download_path='', token=token)
