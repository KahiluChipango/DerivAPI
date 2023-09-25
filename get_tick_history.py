import asyncio
import sqlite3
from deriv_api import DerivAPI
from datetime import datetime

app_id = 1089

# Create a connection to the SQLite database
conn = sqlite3.connect('tick_data.db')
cursor = conn.cursor()

# Create a table to store tick data
cursor.execute('''
    CREATE TABLE IF NOT EXISTS tick_data (
        timestamp INTEGER,
        price REAL
    )
''')
conn.commit()

async def get_tick_history():
    api = DerivAPI(app_id=app_id)
    chunk_size = 500

    while True:
        # Retrieve the lowest timestamp froshoom the database
        cursor.execute('SELECT MIN(timestamp) FROM tick_data')
        min_timestamp = cursor.fetchone()[0]

        # Request tick history
        tick_history_request = {
            'ticks_history': '1HZ10V',  # Replace 'R_100' with the desired symbol
            'count': chunk_size,  # Number of ticks to retrieve
            'end': min_timestamp if min_timestamp else 'latest',  # Use the lowest timestamp or 'latest' if not available
            'granularity': 60,  # Tick interval in seconds
        }

        tick_history = await api.ticks_history(tick_history_request)

        if not tick_history['history']['prices']:
            break

        for price, timestamp in zip(tick_history['history']['prices'], tick_history['history']['times']):
            formatted_datetime = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            print(f"Timestamp: {formatted_datetime}, Price: {price}")

            # Insert tick data into the SQLite database
            cursor.execute('INSERT INTO tick_data (timestamp, price) VALUES (?, ?)', (timestamp, price))
            conn.commit()

        await asyncio.sleep(1)  # Add a small delay to avoid overloading the API

    await api.clear()

asyncio.run(get_tick_history())

# Close the database connection
conn.close()
