import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="PropReturns",
    user="postgres",
    password="Dhyey@16",
    port=5436
)

# Create a cursor
cursor = conn.cursor()

# Check if the table exists
cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'mbprop')")
table_exists = cursor.fetchone()[0]

if table_exists:
    print("The 'MBProp' table exists.")

    # Fetch and print some sample data
    cursor.execute("SELECT * FROM MBProp LIMIT 10")
    sample_data = cursor.fetchall()
    for row in sample_data:
        print(row)
else:
    print("The 'MBProp' table does not exist.")

# Close the cursor and connection
cursor.close()
conn.close()
