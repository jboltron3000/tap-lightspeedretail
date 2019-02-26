from datetime import datetime, date, timedelta
import pendulum
import singer
import time
from singer import bookmarks as bks_
from http import *
from singer import metrics
import pdb
import strict_rfc3339
import json
from .context import Stream


class Order(Stream):
    def __init__(self, Stream):
        #pdb.set_trace()
        super().__init__(Stream.config, Stream.state)
        super().write_page('Order')
        
    def create_relation(self, start_date):
        relation = "&archived=true&load_relations=%5B%22OrderLines%22%5D&timeStamp=%3E," +start_date
        return relation
        
    def paginate(self, offset, count, ext_time, path, stream_id):
        if len(self.state) < 14:
            start_date = singer.utils.strptime_with_tz(self.config['start_date'])
        else:
            first_time = False
            start_date = singer.utils.strptime_with_tz(self.state[stream_id])
        start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S')
        ext_time = start_date 
        num = str(time.time())
        while (int(count) > int(offset) and (int(count) - int(offset)) >= -100):    
            url = "https://api.merchantos.com/API/Account/" + str(self.config['customer_ids']) + "/" + str(stream_id) + ".json?offset="
            relation = self.create_relation(start_date)
            page = self.client.request(stream_id, "GET", (url + str(offset) + relation))
            info = page['@attributes']
            count = info['count']
            if int(count) == 0:
                offset = 0
                continue
            elif int(count) <= 100:
                offset = 300
                data = page[str(stream_id)]  
            else:
                offset = int(info['offset']) + 100
                data = page[str(stream_id)]  
            for key in list(data):
                #pdb.set_trace()
                if 'OrderLines' not in key:
                    pass
                elif int(count) == 1: 
                    dict = data['OrderLines']['OrderLine']
                    for item in list(dict):
                        if type(item) == str:
                            dict['batchid'] = num
                        else:
                            item['batchid'] = num
                else:
                    dict = key['OrderLines']['OrderLine']
                    for item in list(dict):
                        if type(item) == str:
                            dict['batchid'] = num
                        else:
                            item['batchid'] = num
                if type(key) == str:
                    data['batchid'] = num
                    if data['timeStamp'] >= ext_time:
                        ext_time = data['timeStamp']
                    else:
                        pass
                    singer.write_record(stream_id, data)
                    with metrics.record_counter(stream_id) as counter:
                         counter.increment(len(page))
                    continue
                elif str(stream_id) == "Order": 
                    key['batchid'] = num
                    if key['timeStamp'] >= ext_time:
                        ext_time = key['timeStamp']
                    else:
                        pass
                singer.write_record(stream_id, key)
                with metrics.record_counter(stream_id) as counter:
                     counter.increment(len(page))
            path.append(ext_time)
            self.update_start_date_bookmark(path, str(stream_id))
