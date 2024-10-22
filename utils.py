import h5py
import rasterio
from pyproj import CRS
from rasterio.transform import Affine
import numpy as np
from rasterio.warp import calculate_default_transform, reproject, Resampling
import os


def read_hdf_from_oss(bucket, oss_url, tmp_hdf):
    bucket.get_object_to_file(oss_url, tmp_hdf)
    return tmp_hdf


def read_h5_data(filename):
    # open HDF5 file
    with h5py.File(filename, 'r') as f:
        # list sub-dataset
        def print_name(name):
            print(name)

        # f.visit(print_name)
        emis_mean = f['Emissivity']['Mean'][:]
        emis_std = f['Emissivity']['SDev'][:]
        latitude = f['Geolocation']['Latitude'][:]
        longitude = f['Geolocation']['Longitude'][:]
        observations = f['Observations']['NumObs'][:]
        water_map = f['Land Water Map']['LWmap'][:]
        ndvi = f['NDVI']['Mean'][:]

    res = {
        'tir_mean': emis_mean,
        'tir_std': emis_std,
        'latitude': latitude,
        'longitude': longitude,
        'observations': observations,
        'ndvi': ndvi,
        'ndwi': water_map
    }
    return res


def get_rasterio_meta(latitude, longitude):
    h, w = latitude.shape
    pixelHeight = (np.max(latitude) - np.min(latitude)) / (h - 1)
    pixelWidth = (np.max(longitude) - np.min(longitude)) / (w - 1)
    originX = longitude[0, 0]
    originY = latitude[0, 0]
    rotationX = 0
    rotationY = 0
    affine = Affine(pixelWidth, rotationX, originX, rotationY, -pixelHeight, originY)
    return affine


def writeindex2GeoTiff(output_file, data, affine, crs=CRS.from_epsg(4326), dtype=rasterio.int16,
                       reproject2epsg3857=True):
    descriptions = [
        "Garnet Index: (b12 + b14) / b13) *1000 -> Int16",
        "Mafic Mineral Index: (b12 / b13) * (b14 / b13) *1000 ->Int16",
        "Quartz Bearing Rock Index: (b10 / b12) * (b13 / b12) *1000 ->Int16",
        "Quartz Index: (b11 * b11) / (b10 * b12) *1000 ->Int16",
        "Carbonate Index: ((b12 + b14) / b13) *1000 ->Int16",

    ]
    out_meta = {
        "driver": "GTiff",
        "height": data.shape[1],
        "width": data.shape[2],
        "transform": affine,
        "dtype": dtype,
        "crs": crs,
        "count": data.shape[0]
    }

    # Save as EPSG:4326 file
    with rasterio.open(output_file, "w", **out_meta) as dest:
        dest.write(data)

    # reproject to EPSG:3857 if needed
    if reproject2epsg3857:
        dst_crs = CRS.from_epsg(3857)
        temp_file = output_file[:-3] + '_3857.tif'
        # reopen EPSG:4326 file and reproject to 3857
        with rasterio.open(output_file) as src:
            transform, width, height = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *src.bounds)

            out_meta.update({
                "crs": dst_crs,
                "transform": transform,
                "width": width,
                "height": height
            })

            # overwrite
            with rasterio.open(temp_file, "w", **out_meta) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.nearest
                    )
                for i in range(1, data.shape[0] + 1):
                    dst.update_tags(i, DESCRIPTION=descriptions[i - 1])
        # Delete the 4326 file
        os.remove(output_file)
        os.rename(temp_file, output_file)


def writearray2GeoTiff(output_file, data, affine, crs=CRS.from_epsg(4326), dtype=rasterio.int16,
                       reproject2epsg3857=True):
    out_meta = {
        "driver": "GTiff",
        "height": data.shape[1],
        "width": data.shape[2],
        "transform": affine,
        "dtype": dtype,
        "crs": crs,
        "count": data.shape[0]
    }
    descriptions = [
        "TIR Band 10 [Aster GEDv3] -> Int16",
        "TIR Band 11 [Aster GEDv3] -> Int16",
        "TIR Band 12 [Aster GEDv3] -> Int16",
        "TIR Band 13 [Aster GEDv3] -> Int16",
        "TIR Band 14 [Aster GEDv3] -> Int16",
    ]

    # Save as EPSG:4326 file
    with rasterio.open(output_file, "w", **out_meta) as dest:
        dest.write(data)

    # reproject to EPSG:3857 if needed
    if reproject2epsg3857:
        dst_crs = CRS.from_epsg(3857)
        temp_file = output_file[:-3] + '_3857.tif'
        # reopen EPSG:4326 file and reproject to 3857
        with rasterio.open(output_file) as src:
            transform, width, height = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *src.bounds)

            out_meta.update({
                "crs": dst_crs,
                "transform": transform,
                "width": width,
                "height": height
            })

            # overwrite
            with rasterio.open(temp_file, "w", **out_meta) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.nearest
                    )
                for i in range(1, data.shape[0] + 1):
                    dst.update_tags(i, DESCRIPTION=descriptions[i - 1])

        # Delete the 4326 file
        os.remove(output_file)
        os.rename(temp_file, output_file)


def writeobs2GeoTiff(output_file, data, affine, crs=CRS.from_epsg(4326), dtype=rasterio.int16,
                     reproject2epsg3857=True):
    out_meta = {
        "driver": "GTiff",
        "height": data.shape[1],
        "width": data.shape[2],
        "transform": affine,
        "dtype": dtype,
        "crs": crs,
        "count": 1
    }
    descriptions = [
        "TIR Observations [Aster GEDv3] -> Int16",
    ]

    # Save as EPSG:4326 file
    with rasterio.open(output_file, "w", **out_meta) as dest:
        dest.write(data)

    # reproject to EPSG:3857 if needed
    if reproject2epsg3857:
        dst_crs = CRS.from_epsg(3857)
        temp_file = output_file[:-3] + '_3857.tif'
        # reopen EPSG:4326 file and reproject to 3857
        with rasterio.open(output_file) as src:
            transform, width, height = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *src.bounds)

            out_meta.update({
                "crs": dst_crs,
                "transform": transform,
                "width": width,
                "height": height
            })

            # overwrite
            with rasterio.open(temp_file, "w", **out_meta) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.nearest
                    )
                for i in range(1, data.shape[0] + 1):
                    dst.update_tags(i, DESCRIPTION=descriptions[i - 1])

        # Delete the 4326 file
        os.remove(output_file)
        os.rename(temp_file, output_file)


def writestd2GeoTiff(output_file, data, affine, crs=CRS.from_epsg(4326), dtype=rasterio.int16,
                     reproject2epsg3857=True):
    out_meta = {
        "driver": "GTiff",
        "height": data.shape[1],
        "width": data.shape[2],
        "transform": affine,
        "dtype": dtype,
        "crs": crs,
        "count": data.shape[0]
    }
    descriptions = [
        "TIR std Band 10 [Aster GEDv3] -> Float32",
        "TIR std Band 11 [Aster GEDv3] -> Float32",
        "TIR std Band 12 [Aster GEDv3] -> Float32",
        "TIR std Band 13 [Aster GEDv3] -> Float32",
        "TIR std Band 14 [Aster GEDv3] -> Float32",

    ]

    # Save as EPSG:4326 file
    with rasterio.open(output_file, "w", **out_meta) as dest:
        dest.write(data)

    # reproject to EPSG:3857 if needed
    if reproject2epsg3857:
        dst_crs = CRS.from_epsg(3857)
        temp_file = output_file[:-3] + '_3857.tif'
        # reopen EPSG:4326 file and reproject to 3857
        with rasterio.open(output_file) as src:
            transform, width, height = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *src.bounds)

            out_meta.update({
                "crs": dst_crs,
                "transform": transform,
                "width": width,
                "height": height
            })

            # overwrite
            with rasterio.open(temp_file, "w", **out_meta) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.nearest
                    )
                for i in range(1, data.shape[0] + 1):
                    dst.update_tags(i, DESCRIPTION=descriptions[i - 1])
        # Delete the 4326 file
        os.remove(output_file)
        os.rename(temp_file, output_file)


def calc_index(arr):
    b10 = arr[0].astype(np.float64)
    b11 = arr[1].astype(np.float64)
    b12 = arr[2].astype(np.float64)
    b13 = arr[3].astype(np.float64)
    b14 = arr[4].astype(np.float64)
    nodata_value = -9999
    # nodata_mask = b10[b10==nodata_value]*b11[b11==nodata_value]*b12[b12==nodata_value]*b13[b13==nodata_value]*b14[b14==nodata_value]
    nodata_mask = np.where(b10 == nodata_value, 0, 1) * np.where(b11 == nodata_value, 0, 1) * np.where(
        b12 == nodata_value, 0, 1) * np.where(b13 == nodata_value, 0, 1) * np.where(b14 == nodata_value, 0, 1)

    garnet_index = np.array(((b12 + b14) / b13), dtype=np.float64) * nodata_mask
    # garnet_index[garnet_index==nodata_mask]=nodata_value

    carbonate_index = np.array((b13 / b14), dtype=np.float64) * nodata_mask
    # carbonate_index[carbonate_index==nodata_mask]=nodata_value

    mineral_index = np.array((b12 / b13) * (b14 / b13), dtype=np.float64) * nodata_mask
    # mineral_index[mineral_index==nodata_mask]=nodata_value

    rock_index = np.array((b10 / b12) * (b13 / b12), dtype=np.float64) * nodata_mask
    # rock_index[rock_index==nodata_mask]=nodata_value
    qi = np.array((b11 * b11) / (b10 * b12), dtype=np.float64) * nodata_mask
    # qi[qi==nodata_mask]=nodata_value
    '''
    Garnet index, 
    Mafic mineral index, 
    Quartz mineral rock index, 
    Quartz index, 
    Carbonate index
    '''

    rock_index[np.isnan(rock_index)] = 0
    mineral_index[np.isnan(mineral_index)] = 0
    qi[np.isnan(qi)] = 0
    garnet_index[np.isnan(garnet_index)] = 0
    carbonate_index[np.isnan(carbonate_index)] = 0

    index_result = np.stack(
        [garnet_index * 1000, mineral_index * 1000, rock_index * 1000, qi * 1000, carbonate_index * 1000, ])

    return index_result
