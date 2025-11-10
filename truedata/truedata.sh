
/usr/local/bin/pm2 stop trueData_service
/usr/local/bin/pm2 delete trueData_service
/root/ParallelCandles/truedata/venv/bin/python3 /root/ParallelCandles/truedata/get_symbol_list_ohlc.py
/usr/local/bin/pm2 start "/root/ParallelCandles/truedata/venv/bin/python3 /root/ParallelCandles/truedata/td4_simple.py" --name trueData_service