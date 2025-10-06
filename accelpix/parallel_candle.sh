
# Clean old candle service
pm2 stop candle_service || true
pm2 delete candle_service || true

# Start long-running candle service
pm2 start "/root/ParallelCandles/accelpix/venv/bin/python3 /root/ParallelCandles/accelpix/parallel_2.py" --name candle_service
