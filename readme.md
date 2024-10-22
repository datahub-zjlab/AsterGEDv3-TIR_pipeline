### **Purpose**

This project is to download, process AsterGEDv3 dataset, and save them to oss.
The output files are as follows:
1. TIR Dataset (using TIF format)
2. Error indicator Dataset (observation and Emissivity SDev)
3. Group indexes calculated by Aster Channel 10, 11, 12, 13, 14

<img src="assets/TIR Band 10-12.png" width="900px"/>
<img src="assets/Quarty bearing rock index.png" width="900px"/>


### **Get Started**

1. Install Packages  

`pip install -r requirements.txt`

2. Check the main functions from _example.ipynb_

3. After config the oss and related tokens in _config_save.py_, run the main script 

`python TIR_Pipeline_multiprocessor.py`
### **Method**

ASTER-GEDV3 data is product-level data for the Aster thermal infrared 5-channel bands, jointly released by NASA's Jet Propulsion Laboratory (JPL) (http://10.200.48.72:30000/portal/dataset?id=1821942), named b10, b11, b12, b13, and b14, respectively. After the following calculations, the 5-channel functional group data is obtained, which constitutes this dataset.

1. Geographic coordinate transformation: Resampling ASTER-GEDV3 data (EPSG: 4326) to geographic space (EPSG: 3857);
2. Functional group calculation (according to the formulas below), stored as float64;
3. Outlier correction: Setting nodata values (-9999) from the original data and NAN values generated during functional group calculations to 0;
4. Data format modification: Multiplying functional group calculation values by 1000 and storing them in Int16 format.

The five functional group indicators calculated from ASTER-GEDV3 thermal infrared data.

| **Index ID** | **Index Name**                | **Equation**               |
| -------------------- | ------------------------------------- | ---------------------------------- |
| **1**        | **Garnet Index**              | **((b12 + b14) / b13)**          |
| **2**        | **Mafic Mineral Index**       | **((b12 / b13) \* (b14 / b13)**  |
| **3**        | **Quartz Bearing Rock Index** | **((b10 / b12) \* (b13 / b12))** |
| **4**        | **Quartz Index**              | **(b11^2 / (b10 \* b12))**       |
| **5**        | **Carbonate Index**           | **(b13 / b14)**                  |

### **Related Material**

AsterGEDv3 homepage:https://lpdaac.usgs.gov/products/ag100v003/, 

AsterGEDv3 Datapool(100m resolution):https://e4ftl01.cr.usgs.gov/ASTT/AG100.003/2000.01.01/

### **Contact us**
[Hu Jinnan ](jinnanhu@zhejianglab.com), Zhejiang Lab, CHINA




