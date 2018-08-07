from datetime import datetime, date, timedelta
import pendulum
import singer
from singer import bookmarks as bks_
from .http import Client
from singer import metrics
import pdb
import strict_rfc3339
import json

class Context(object):
    """Represents a collection of global objects necessary for performing
    discovery or for running syncs. Notably, it contains

    - config  - The JSON structure from the config.json argument
    - state   - The mutable state dict that is shared among streams
    - client  - An HTTP client object for interacting with the API
    - catalog - A singer.catalog.Catalog. Note this will be None during
                discovery.
    """
    def __init__(self, config, state):
       	self.config = config
        self.state = state
        self.client = Client(config)
        self._catalog = None
        self.selected_stream_ids = None
        self.now = datetime.utcnow()
        self.first_time = True
        
    @property
    def catalog(self):
        return self._catalog

    @catalog.setter
    def catalog(self, catalog):
        self._catalog = catalog
        self.selected_stream_ids = set(
            [s.tap_stream_id for s in catalog.streams
             if s.is_selected()]
        )

    def get_bookmark(self, path):
        return bks_.get_bookmark(self.state, *path)

    def bookmark(self, path, stream_id):
        bookmark = self.state
        #pdb.set_trace()
        for p in path:
            if p not in bookmark:
                if self.first_time:
                    bookmark['type'] = "STATE"
                    bookmark[stream_id] = p 
                else:
                    del bookmark[stream_id]
                    bookmark['type'] = "STATE"
                    bookmark[stream_id] = p  
        return bookmark
        
    def set_bookmark(self, path, val):
        if isinstance(val, date):
            val = val.isoformat()
        bks_.write_bookmark(self.state, path[0], path[1], val)
        

    def get_offset(self, path):
        off = bks_.get_offset(self.state, path[0])
        return (off or {}).get(path[1])

    def set_offset(self, path, val):
        bks_.set_offset(self.state, path[0], path[1], val)

    def clear_offsets(self, tap_stream_id):
        bks_.clear_offset(self.state, tap_stream_id)

    def update_start_date_bookmark(self, path, stream_id):
        val = self.bookmark(path, stream_id)
        if not val:
            val = self.config["start_date"]
            self.set_bookmark(path, val)
        return val
        

    def write_page(self, stream_id):
        count = 100
        offset = 0
        path = []
        ext_time = self.config['start_date']
        if len(self.state) < 12:
            start_date = singer.utils.strptime_with_tz(self.config['start_date'])
        else:
            first_time = False
            start_date = singer.utils.strptime_with_tz(self.state[stream_id])
        end_date = singer.utils.now()
        start_date = start_date.strftime('%m/%d/%Y')
        end_date = end_date.strftime('%m/%d/%Y')
        relation = ""
        if str(stream_id) == "Item":
            relation = "&load_relations=%5B%22ItemShops%22%2C%22ItemAttributes%22%2C%22Tags%22%5D&timeStamp=%3E%3D," +start_date
        elif str(stream_id) == "Shop":
            relation = ""
        elif str(stream_id) == "Sale":
            relation = "&load_relations=%5B%22Customer.Contact%22%5D&timeStamp=%3E%3D," +start_date
        elif str(stream_id) == "SaleLine":
            relation = "&load_relations=%5B%22TaxClass%22%5D&timeStamp=%3E%3D," +start_date
        elif str(stream_id) == "ItemMatrix":
            relation = "&load_relations=%5B%22TaxClass%22%5D&timeStamp=%3E%3D," +start_date
        elif str(stream_id) == "Employee":
            relation = "&load_relations=%5B%22EmployeeRole%22%5D&timeStamp=%3E%3D," +start_date
        elif str(stream_id) == "Register":
            relation = ""
        else:
            relation = "&timeStamp=%3E%3D," +start_date
        while int(count) > int(offset) and (int(count) - int(offset)) > -100:
            page = self.client.request(stream_id, "GET", "https://api.merchantos.com/API/Account/" + str(self.config['customer_ids']) + "/" + str(stream_id) + ".json?offset=" + str(offset) + relation)
            info = page['@attributes']
            data = page[str(stream_id)]
            count = info['count']
            offset = int(info['offset']) + 100
            for key in data:
                if str(stream_id) == "Register": 
                	pass
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
                
            
    def write_state(self):
        singer.write_state(self.state)
        f = open("state.json", 'w')
        message = json.dumps(self.state)
        f.write(str(message))
        f.close()
