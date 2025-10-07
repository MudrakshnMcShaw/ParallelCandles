pm2 stop accelpix_data_service || true
pm2 delete accelpix_data_service || true

/root/ParallelCandles/accelpix/venv/bin/python3 /root/ParallelCandles/accelpix/download_json.py
/root/ParallelCandles/accelpix/venv/bin/python3 /root/ParallelCandles/accelpix/get_symbol_list_ohlc.py
/root/ParallelCandles/accelpix/venv/bin/python3 /root/ParallelCandles/accelpix/map.py

pm2 start "/root/ParallelCandles/accelpix/venv/bin/python3 /root/ParallelCandles/accelpix/data2.py" --name accelpix_data_service