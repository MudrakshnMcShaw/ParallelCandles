/usr/local/bin/pm2 stop app_service || true
/usr/local/bin/pm2 delete app_service || true

/usr/local/bin/pm2 start "/root/ParallelCandles/accelpix/venv/bin/python3 /root/ParallelCandles/accelpix/app.py" --name app_service