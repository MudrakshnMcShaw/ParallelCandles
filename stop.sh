#!/bin/bash
date
/usr/local/bin/pm2 stop trueData_service
/usr/local/bin/pm2 stop candle_service
/usr/local/bin/pm2 stop accelpix_data_service