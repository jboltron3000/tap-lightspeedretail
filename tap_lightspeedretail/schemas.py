#!/usr/bin/env python3
import os
import singer
from singer import utils

## What is this?
class IDS(object):
    table1 = "Order"
    table2 = "Item"
	

stream_ids = ["Order", "Item"]

pk_fields = {
    IDS.table1: ["orderID"],
	IDS.table2: ["itemID"],
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
