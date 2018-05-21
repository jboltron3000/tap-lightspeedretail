import singer
from .schemas import IDS
from .http import Paginator
import pdb

LOGGER = singer.get_logger()


def metrics(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))

def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
    metrics(tap_stream_id, records)

def sync_lists(ctx):
    for tap_stream_id in ctx.selected_stream_ids:
        if tap_stream_id == "Item":
            ctx.write_page_item(tap_stream_id)
        elif tap_stream_id == "Customer":
            ctx.write_page_customer(tap_stream_id)
        elif tap_stream_id == "Sale":
            ctx.write_page_sale(tap_stream_id)
        elif tap_stream_id == "SaleLine":
            ctx.write_page_saleLine(tap_stream_id)
        elif tap_stream_id == "Shop":
            ctx.write_page_shop(tap_stream_id)
        elif tap_stream_id == "Vendor":
            ctx.write_page_vendor(tap_stream_id)
        else:
            ctx.write_page_order(tap_stream_id)
            

class Stream(object):
    Order = [IDS.table1, ["orderID"]]
    Item = [IDS.table2, ["itemID"]]
    Customer = [IDS.table3, ["customerID"]]
    Sale = [IDS.table4, ["saleID"]]
    SaleLine = [IDS.table5, ["saleLineID"]]
    Shop = [IDS.table6, ["shopID"]]
    Vendor = [IDS.table7, ["vendorID"]]
    
    def write_page(self, page):
        singer.write_records(self.tap_stream_id, page)
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))

class Everything(Stream):
    def __init__(self, *args, path, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path

    def sync(self, ctx):
        page = ctx.client.request(self.tap_stream_id, "GET", self.path)
        self.write_page(page)
