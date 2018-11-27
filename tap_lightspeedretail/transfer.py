from datetime import datetime, date, timedelta
import pendulum
import singer
from singer import bookmarks as bks_
from http import *
from singer import metrics
import pdb
import strict_rfc3339
import json
from .context import Stream


class Transfer(Stream):
    def __init__(self, Stream):
        super().__init__(Stream.config, Stream.state)
        super().write_page('Transfer')
        
        
    def create_relation(self, stream_id, start_date):
        relation = ""
        if str(stream_id) == "Transfer":
            relation = "&archived=true&orderby=transferID&timeStamp=%3E," +start_date 
            stream_id = "Inventory/Transfer"
        elif str(stream_id) == "TransferItem" or str(stream_id) == "Inventory/Transfer/" + self.id + "/TransferItems":
            relation = "&archived=true&orderby=transferItemID&timeStamp=%3E," +start_date 
            stream_id = "Inventory/Transfer/" + self.id + "/TransferItems"    
        return relation
        
    def paginate(self, offset, count, ext_time, path, stream_id):
        if len(self.state) < 14:
            start_date = singer.utils.strptime_with_tz(self.config['start_date'])
        else:
            first_time = False
            start_date = singer.utils.strptime_with_tz(self.state[stream_id])
        start_date = start_date.strftime('%m/%d/%YT%H:%M:%S')
        ext_time = start_date 
        if stream_id == "Transfer":
            stream_id = "Inventory/Transfer"
        elif stream_id == "TransferItem":
            stream_id = "Inventory/Transfer/" + self.id + "/TransferItems"
        while (int(count) > int(offset) and (int(count) - int(offset)) >= -100) and stream_id != ("Inventory/Transfer//TransferItems"):    
            #pdb.set_trace()
            url = "https://api.merchantos.com/API/Account/" + str(self.config['customer_ids']) + "/" + str(stream_id) + ".json?offset="
            if stream_id == "Inventory/Transfer/" + self.id + "/TransferItems":
                relation = self.create_relation("TransferItem", start_date)
            elif stream_id == "Inventory/Transfer":
                relation = self.create_relation("Transfer", start_date)
            else:
                relation = self.create_relation(stream_id, start_date)
            page = self.client.request(stream_id, "GET", (url + str(offset) + relation))
            if stream_id == "Inventory/Transfer/" + self.id + "/TransferItems":
                stream_id = "TransferItem"
                self.id = ""
                #pdb.set_trace()
            elif stream_id == "Inventory/Transfer":
                stream_id = "Transfer"
            info = page['@attributes']
            count = info['count']
            if int(count) == 0:
                offset = 0
                self.id = ""
                stream_id = "Inventory/Transfer/" + self.id + "/TransferItems"
                continue
            elif int(count) <= 100:
                offset = 300
                data = page[str(stream_id)]  
            else:
                offset = int(info['offset']) + 100
                data = page[str(stream_id)]  
            for key in data:
                if type(key) == str and str(stream_id) == "Transfer" :
                    self.id = data['transferID']
                    super().write_page('TransferItem')
                    if data['timeStamp'] >= ext_time:
                        ext_time = data['timeStamp']
                    else:
                        pass
                    singer.write_record(stream_id, data)
                    with metrics.record_counter(stream_id) as counter:
                         counter.increment(len(page))
                    continue
                elif type(key) == str and str(stream_id) == "TransferItem":
                    if data['timeStamp'] >= ext_time:
                        ext_time = data['timeStamp']
                    else:
                        pass
                    singer.write_record(stream_id, data)
                    with metrics.record_counter(stream_id) as counter:
                         counter.increment(len(page))
                    continue
                elif str(stream_id) == "Transfer": 
                    self.id = key['transferID']
                    super().write_page('TransferItem')
                    if key['timeStamp'] >= ext_time:
                        ext_time = key['timeStamp']
                else:
                    if key['timeStamp'] >= ext_time:
                        ext_time = key['timeStamp']
                    else:
                        pass
                singer.write_record(stream_id, key)
                with metrics.record_counter(stream_id) as counter:
                     counter.increment(len(page))
            path.append(ext_time)
            self.update_start_date_bookmark(path, str(stream_id))
            if stream_id == "Transfer":
                stream_id = "Inventory/Transfer"
            elif stream_id == "TransferItem":
                offset = 0
                #pdb.set_trace()
                stream_id = "Inventory/Transfer/" + self.id + "/TransferItems"
                