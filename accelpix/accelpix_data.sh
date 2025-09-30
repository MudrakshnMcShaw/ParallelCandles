pm2 stop accelpix_data_service || true
pm2 delete accelpix_data_service || true

/root/parallel_candle/accelpix/venv/bin/python3 /root/parallel_candle/accelpix/download_json.py
/root/parallel_candle/accelpix/venv/bin/python3 parallel_candle/accelpix/get_symbol_list_ohlc.py
/root/parallel_candle/accelpix/venv/bin/python3 /root/parallel_candle/accelpix/map.py

pm2 start "/root/parallel_candle/accelpix/venv/bin/python3 /root/parallel_candle/accelpix/data2.py" --name accelpix_data_service
pm2 save
