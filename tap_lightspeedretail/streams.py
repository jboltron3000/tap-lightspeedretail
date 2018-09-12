import singer
from .schemas import IDS
import pdb

LOGGER = singer.get_logger()


def metrics(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))

def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
    metrics(tap_stream_id, records)

def sync_lists(ctx):
    ctx.write_page("Transfer")
    ctx.selected_stream_ids.remove("Transfer")
    ctx.write_page("TransferItem")
    ctx.selected_stream_ids.remove("TransferItem")
    for tap_stream_id in ctx.selected_stream_ids:
        ctx.write_page(tap_stream_id)
            

class Stream(object):
    Order = [IDS.table1, ["orderID"]]
    Item = [IDS.table2, ["itemID"]]
    Customer = [IDS.table3, ["customerID"]]
    Sale = [IDS.table4, ["saleID"]]
    SaleLine = [IDS.table5, ["saleLineID"]]
    Shop = [IDS.table6, ["shopID"]]
    Vendor = [IDS.table7, ["vendorID"]]
    Category = [IDS.table8, ["categoryID"]]
    ItemMatrix = [IDS.table9, ["itemMatrixID"]]
    Employee = [IDS.table10, ["employeeID"]]
    Register = [IDS.table11, ["registerID"]]
    OrderLine = [IDS.table12, ["orderLineID"]]
    Transfer = [IDS.table13, ["transferID"]]
    TransferItem = [IDS.table14, ["transferItemID"]]
    VendorReturn = [IDS.table15, ["vendorReturnID"]]
   
    
    def write_page(self, page):
        singer.write_records(self.tap_stream_id, page)
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))

