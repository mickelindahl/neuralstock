import os
from dotenv import load_dotenv
import psycopg2
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Configure matplotlib backend to display plots in an external window
plt.switch_backend('TkAgg')

# Load environment variables
load_dotenv()

# Connect to your postgres DB
conn = psycopg2.connect(
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
)

# Open a cursor to perform database operations
cur = conn.cursor()

# Query the intraday data from the table
cur.execute("SELECT time, open, high, low, close, volume FROM intraday ORDER BY time")

# Fetch all the rows
rows = cur.fetchall()

# Close the cursor and connection
cur.close()
conn.close()

# Extract the columns from the rows
time = [row[0] for row in rows]
open_values = [row[1] for row in rows]
high_values = [row[2] for row in rows]
low_values = [row[3] for row in rows]
close_values = [row[4] for row in rows]
volume = [row[5] for row in rows]

# Convert time to a numeric format
time_numeric = mdates.date2num(time)

# Sort the data by time
time_numeric, open_values, high_values, low_values, close_values, volume = zip(*sorted(zip(time_numeric, open_values, high_values, low_values, close_values, volume)))

# Plot the data using Matplotlib
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

# Plot volume
ax1.plot(time_numeric, volume, label='Volume')
ax1.set_ylabel('Volume')

# Plot close values
ax2.plot(time_numeric, open_values, label='Open')
ax2.plot(time_numeric, high_values, label='High')
ax2.plot(time_numeric, low_values, label='Low')
ax2.plot(time_numeric, close_values, label='Close')
ax2.set_xlabel('Time')
ax2.set_ylabel('Price')

# Format xtick labels with date strings
date_format = mdates.DateFormatter('%Y-%m-%d %H:%M:%S')
ax2.xaxis.set_major_formatter(date_format)
fig.autofmt_xdate()

# Set xtick locations
ax2.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=3, maxticks=7))

# Add legends
ax1.legend()
ax2.legend()

# Show the plot in an external window
plt.show()



