from datetime import datetime, timezone, timedelta
print(int(datetime.now(timezone(timedelta(hours=5, minutes=30))).timestamp() * 1000))
