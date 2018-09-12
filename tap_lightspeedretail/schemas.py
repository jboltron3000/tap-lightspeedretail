#!/usr/bin/env python3
import os
import singer
from singer import utils

## What is this?
class IDS(object):
    table1 = "Order"
    table2 = "Item"
    table3 = "Customer"
    table4 = "Sale"
    table5 = "SaleLine"
    table6 = "Shop"
    table7 = "Vendor"
    table8 = "Category"
    table9 = "ItemMatrix"
    table10 = "Employee"
    table11 = "Register"
    table12 = "OrderLine"
    table13 = "Transfer"
    table14 = "TransferItem"
    table15 = "VendorReturn"

	

stream_ids = ["Order", "Item", "VendorReturn", "Customer", "Sale", "SaleLine", "Shop", "Vendor", "Category", "ItemMatrix", "Employee", "Register", "Transfer", "OrderLine", "TransferItem"]

pk_fields = {
    IDS.table1: ["orderID"],
	IDS.table2: ["itemID"],
	IDS.table3: ["customerID"],
	IDS.table4: ["saleID"],
	IDS.table5: ["saleLineID"],
	IDS.table6: ["shopID"],
	IDS.table7: ["vendorID"],
	IDS.table8: ["categoryID"],
	IDS.table9: ["itemMatrixID"],
	IDS.table10: ["employeeID"],
	IDS.table11: ["registerID"],
	IDS.table12: ["orderLineID"],
	IDS.table13: ["transferID"],
	IDS.table14: ["transferItemID"],
	IDS.table15: ["vendorReturnID"],
	
	
}
##
def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
    
def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    return schema

def load_and_write_schema(tap_stream_id):
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, pk_fields[tap_stream_id])
