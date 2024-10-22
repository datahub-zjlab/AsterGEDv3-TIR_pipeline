'''
Contact Hu Jinnan(jinnanhu@zhejianglab.com) for details
'''

import oss2
import urllib.parse

region = ''
endpoint = ''
src_accessKeyId = ''
src_accessKeySecret = ''
src_bucket_name = ''
src_auth = oss2.Auth(src_accessKeyId, src_accessKeySecret)
src_bucket = oss2.Bucket(src_auth, endpoint, src_bucket_name, region=region)
dest_accessKeyId = ''
dest_accessKeySecret = ''
dest_bucket_name = ''
dest_auth = oss2.Auth(dest_accessKeyId, dest_accessKeySecret)
dest_bucket = oss2.Bucket(dest_auth, endpoint, dest_bucket_name, region=region)

# token for downloading AsterGEDv3 files
token = ''

# nasa account
username = ''
password = urllib.parse.quote('')  # 对密码中的特殊字符进行URL编码

