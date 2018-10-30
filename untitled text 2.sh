#!/bin/bash

cd tap-lightspeedretail/
python3 -m venv ~/.virtualenvs/tap-lightspeedretail
source ~/.virtualenvs/tap-lightspeedretail/bin/activate
pip install -e .


 if start_date == end_date:
                start_date += timedelta(+29)
                if start_date > singer.utils.now():
                    start_date = singer.utils.now()
            else:
                start_date =  singer.utils.strptime_with_tz(ctx.state["traffic"])
        end_date = (start_date + timedelta(+29))
        if singer.utils.now() < end_date:
            end_date = singer.utils.now()
