# TODO : may be good idea to use it because we would have similar to url filename
# from slugify import slugify
import json
import hashlib
import urllib3

import os
import errno

datasets_path = None


def set_dataset_path(new_dataset_path):
    global datasets_path
    datasets_path = new_dataset_path


def ensure_dir(directory):
    try:
        os.makedirs(directory)
        print(f'create directory {directory}')
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e
        print(f'directory {directory} already exist')


headers = None
http = urllib3.HTTPConnectionPool('web.archive.org', maxsize=50,
                                  retries=urllib3.Retry(3, redirect=2),
                                  headers=headers)


def get_captures_of_url_and_year(url, year):
    # Collapse captures by timestamp to get 1 capture per hour.
    # Its necessary to reduce the huge number of captures some websites
    # (e.g. twitter.com has 167k captures for 2018. Get only 2xx captures.
    cdx_url = f'/cdx/search/cdx?url={url}&from={year}&to={year}&' \
              'fl=timestamp,digest&collapse=timestamp:10&statuscode=200'
    response = http.request('GET', cdx_url)
    assert response.status == 200
    assert response.data
    captures_txt = response.data.decode('utf-8')
    captures = [l.split(' ') for l in captures_txt.strip().split("\n")]
    return captures


def download_capture(url, timestamp):
    resp = http.request('GET', f'/web/{timestamp}id_/{url}')
    return resp.data.decode('utf-8', 'ignore')


stored_captures = {}


def for_each_capture(url, year):
    """
    TODO: can make it in parallel

    :param url:
    :param year:
    :return:
    """
    captures = get_captures_of_url_and_year(url, year)
    # TODO: can make it in parallel
    for c in captures:
        [timestamp, digest] = c
        duplicated = False
        if digest not in stored_captures:
            response_data = download_capture(url, timestamp)
            # TODO: store to file
            stored_captures[digest] = response_data
        else:
            duplicated = True
            response_data = stored_captures[digest]

        yield (timestamp, digest, response_data, duplicated)


def encode_url_to_path(url):
    if isinstance(url, str):
        url = url.encode('utf-8')
    return hashlib.md5(url).hexdigest()


def store_captures_to_dataset(url, year):
    url_to_path = encode_url_to_path(url)
    dataset_path = f'{datasets_path}/wbm'
    dataset_path_captures = f'{dataset_path}/captures'
    dataset_path_urls = f'{dataset_path}/urls'

    ensure_dir(dataset_path_captures)
    captures = []
    for timestamp, digest, data, duplicated in for_each_capture(url, year):
        print('get data of ', timestamp, 'digest', digest, 'size', len(data), 'duplicate', duplicated)

        # store captures of year
        file_name = f'{dataset_path_captures}/{digest}'
        if not duplicated:
            if not os.path.isfile(file_name):
                with open(file_name, 'w+') as capture_data_file:
                    capture_data_file.write(data)
                print(f'created file {file_name}')
            else:
                print(f'already have {file_name}')
        captures.append([timestamp, digest])

    file_name = f'{dataset_path_urls}/{url_to_path}/{year}'
    if not os.path.isfile(file_name):
        ensure_dir(f'{dataset_path_urls}/{url_to_path}')
        with open(file_name, 'w+') as url_captures_file:
            url_captures_file.write(json.dumps(captures))
        print(f'created file {file_name}')
    else:
        print(f'already have {file_name}')

    print('we got all')
