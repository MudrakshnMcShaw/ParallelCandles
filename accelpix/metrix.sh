/usr/local/bin/pm2 stop metrix_service || true
/usr/local/bin/pm2 delete metrix_service || true

/usr/local/bin/pm2 start "/root/ParallelCandles/accelpix/venv/bin/python3 /root/ParallelCandles/accelpix/metrix.py" --name metrix_service