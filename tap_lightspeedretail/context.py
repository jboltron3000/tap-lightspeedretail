from datetime import datetime, date, timedelta
import pendulum
import singer
from singer import bookmarks as bks_
from .http import Client
from singer import metrics
import pdb
import strict_rfc3339
import json


####################################################################################
##                                                                                ## 
##  Represents a collection of global objects necessary for performing            ## 
##    discovery or for running syncs. Notably, it contains                        ##                   
##                                                                                ##                  
##    - config  - The JSON structure from the config.json argument                ##                           
##    - state   - The mutable state dict that is shared among streams             ##                           
##    - client  - An HTTP client object for interacting with the API              ##                                           
##    - catalog - A singer.catalog.Catalog. Note this will be None during         ##                                                       
##                discovery.                                                      ##                                                         
##              																  ##													 
####################################################################################
    
class Stream(object):
    def __init__(self, config, state):
       	self.config = config
        self.state = state
        self.client = Client(config)
        self._catalog = None
        self.selected_stream_ids = None
        self.now = datetime.utcnow()
        self.first_time = True
        self.id = ""
        self.recurse = False
    
#########################################################################################################
##                                                                                                     ## 
## Bookmark function: Deletes and updates the set saved in Stream.state                                ##
## Necessary for update_start_date_bookmark (see below).                                               ##
##              															   					       ##
#########################################################################################################
    
    def bookmark(self, path, stream_id):
        bookmark = self.state
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
    
##########################################################################################################
##                                                                                                      ## 
## Update Bookmark function: Run after each page is done recording.                                     ##
## If needed, deletes the existing timestamp and replaces it with the later timestamp.                  ##
##              																					    ##
##########################################################################################################
       
    def update_start_date_bookmark(self, path, stream_id):
        val = self.bookmark(path, stream_id)
        if not val:
            val = self.config["start_date"]
            self.set_bookmark(path, val)
        return val
  
###########################################################################################################################
##                                                                                                                       ## 
## Write Page function: Initiates the sync. Calls child method paginate for each instance to start pagination.           ##
## If initial load, state.json should be empty and ext_time will take the place of all timestamps.                       ##
## If not initial load, initial load values will be overwritten.                                                         ##  
##              																										 ##
###########################################################################################################################
          
    def write_page(self, stream_id):
        self.recurse = False
        count = 100
        offset = 0
        path = []
        ext_time = self.config['start_date']
        self.paginate(offset, count, ext_time, path, stream_id) 


########################################################################
##                                                                    ## 
## Write State function: Final step done after sync is done.          ##
## Takes self.state and writes over existing state.json file.         ##
## Uses python built-in functions to execute this process.            ##  
## 																	  ##
########################################################################
           
    def write_state(self):
        singer.write_state(self.state)
        f = open("state.json", 'w')
        message = json.dumps(self.state)
        f.write(str(message))
        f.close()
            