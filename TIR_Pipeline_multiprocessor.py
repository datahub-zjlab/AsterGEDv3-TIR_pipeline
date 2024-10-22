from utils import *
from config_save import src_bucket, dest_bucket
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set logs
logging.basicConfig(filename='tir_pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

filelist_txt = 'AG100_filelist.txt'
file_list = []
with open(filelist_txt, 'r', encoding='utf-8') as file:
    file_list = [line.strip() for line in file]

# config temp folder
tmp_dir = 'E:\\tmp/1014/'

aster_h5_dir = os.path.join(tmp_dir, 'aster_h5_tmp')
aster_tif_dir = os.path.join(tmp_dir, 'aster_tif_tmp')
tir_index_dir = os.path.join(tmp_dir, 'tir_index_tmp')
tir_obs_dir = os.path.join(tmp_dir, 'tir_obs_tmp')
tir_std_dir = os.path.join(tmp_dir, 'tir_std_tmp')

os.makedirs(aster_h5_dir, exist_ok=True)
os.makedirs(aster_tif_dir, exist_ok=True)
os.makedirs(tir_index_dir, exist_ok=True)
os.makedirs(tir_obs_dir, exist_ok=True)
os.makedirs(tir_std_dir, exist_ok=True)


def process_h5_file(row, oss_src_prefix='basic/aster/Aster-GEDv3/h5', oss_dst_prefix='asterpreprocess/tirpipeline'):
    granule_id = file_list[row][:-3]
    # AsterGEDv3 h5 oss
    oss_url = f'{oss_src_prefix}/{granule_id}.h5'

    tmp_h5 = os.path.join(aster_h5_dir, f'{granule_id}.h5')
    tmp_tif = os.path.join(aster_tif_dir, f'{granule_id}.tif')
    tmp_index = os.path.join(tir_index_dir, f'{granule_id}.tif')
    tmp_obs = os.path.join(tir_obs_dir, f'{granule_id}.tif')
    tmp_std = os.path.join(tir_std_dir, f'{granule_id}.tif')

    # if file not exists, fetch it from oss_url
    if not os.path.exists(tmp_h5):
        read_hdf_from_oss(src_bucket, oss_url, tmp_h5)

    h5_data = read_h5_data(tmp_h5)
    latitude, longitude = h5_data['latitude'], h5_data['longitude']
    affine = get_rasterio_meta(latitude, longitude)

    index_result = calc_index(h5_data["tir_mean"])
    writeindex2GeoTiff(tmp_index, index_result, affine, crs=CRS.from_epsg(4326), dtype=rasterio.int16,
                       reproject2epsg3857=False)
    writearray2GeoTiff(tmp_tif, h5_data["tir_mean"], affine, crs=CRS.from_epsg(4326), dtype=rasterio.int16,
                       reproject2epsg3857=False)
    writestd2GeoTiff(tmp_std, h5_data["tir_std"], affine, crs=CRS.from_epsg(4326), dtype=rasterio.float32,
                     reproject2epsg3857=False)
    writeobs2GeoTiff(tmp_obs, np.expand_dims(h5_data["observations"], axis=0), affine, crs=CRS.from_epsg(4326),
                     dtype=rasterio.int16, reproject2epsg3857=False)

    dest_bucket.put_object_from_file(os.path.join(oss_dst_prefix, f'gedv3_tir_4326/{granule_id}.tiff'), tmp_tif)
    dest_bucket.put_object_from_file(os.path.join(oss_dst_prefix, f'tirindex_4326/{granule_id}.tiff'), tmp_index)
    dest_bucket.put_object_from_file(os.path.join(oss_dst_prefix, f'errorindicator_4326/{granule_id}_std.tiff'),
                                     tmp_std)
    dest_bucket.put_object_from_file(os.path.join(oss_dst_prefix, f'errorindicator_4326/{granule_id}_obs.tiff'),
                                     tmp_obs)

    os.remove(tmp_h5)
    os.remove(tmp_std)
    os.remove(tmp_obs)
    os.remove(tmp_tif)
    os.remove(tmp_index)


# Set multi-processor program
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

with ThreadPoolExecutor(max_workers=1) as executor:
    futures = {executor.submit(process_h5_file, i): i for i in range(len(file_list))}

    for future in tqdm(as_completed(futures), total=len(futures)):
        index = futures[future]
        try:
            future.result()
        except Exception as e:
            print(f"{file_list[index]} process error: {e}")
