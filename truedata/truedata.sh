
pm2 stop trueData_service
pm2 delete trueData_service
/root/parallel_candle/truedata/venv/bin/python3 /root/TreuDataStock-master-2/get_symbol_list_ohlc.py
pm2 start "/root/parallel_candle/truedata/venv/bin/python3 /root/parallel_candle/truedata/trueDataLive2.py" --name trueData_service
pm2 save