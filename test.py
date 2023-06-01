from alpha_vantage.timeseries import TimeSeries
import json
import csv
import time

with open('.config.json') as f:
    config = json.load(f)

ts = TimeSeries(key=config['API_KEY'], output_format='csv')
# # Get json object with the intraday data and another with  the call's metadata
# d4ata, meta_data = ts.get_intraday('TSLA', interval='1min')

for year in [1,2]:
    for month in range(1,2):

        # Only goes up to slice year2month12
        slice = 'year{}month{}'.format(year, month)

        symbol = 'TSLA'

        d4ata, meta_data = ts.get_intraday_extended(symbol, interval='1min', slice=slice)
        # d4ata, meta_data = ts.get_intraday(symbol, interval='15min')

        data = {
            'd4data': d4ata,
            'meta_data': meta_data
        }

        name = 'data/tsla/202305/intraday_extended_{}.csv'.format(slice)

        with open(name,'w', newline='') as f:
            writer = csv.writer(f, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for row in d4ata:
                writer.writerow(row)

        time.sleep(15)

# with open('data/tsla/intraday_extended_{}.json'.format(slice), 'w') as f:
#     json.dump(data, f, indent=2)
