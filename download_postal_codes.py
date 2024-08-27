import requests
import time
from multiprocessing import Pool

def pcode_to_data(pcode):
    if int(pcode) % 1000 == 0:
        print(pcode)
    page = 1
    results = []
    outer_loop = True
    inner_loop = True

    while outer_loop:
        try:
            while inner_loop:
                response_raw = requests.get('https://www.onemap.gov.sg/api/common/elastic/search?searchVal={0}&returnGeom=Y&getAddrDetails=Y&pageNum={1}'.format(pcode, page))
                try:
                    response = response_raw.json()
                    inner_loop = False
                except ValueError as e:
                    print('Fetching {} failed due to invalid JSON. Retrying in 2 sec'.format(pcode))
                    time.sleep(2)
                
        except requests.exceptions.ConnectionError as e:
            print('Fetching {} failed. Retrying in 2 sec'.format(pcode))
            time.sleep(2)    
        
        results = results + response['results']
        if response['totalNumPages'] > page:
            page = page + 1
        else:
            outer_loop = False

    return results

import json

if __name__ == '__main__':
    pool = Pool(processes=5)
    
    postal_codes = range(0, 1000000)
    postal_codes = ['{0:06d}'.format(p) for p in postal_codes]

    all_buildings = pool.map(pcode_to_data, postal_codes)
    all_buildings_flattened = [item for sublist in all_buildings[1:] for item in sublist]
    all_buildings_flattened.sort(key=lambda b: (b['POSTAL'], b['SEARCHVAL']))

    with open('buildings.json', 'w') as f:
        f.write(json.dumps(all_buildings_flattened, indent=2, sort_keys=True))

