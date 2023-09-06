from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.proxy import Proxy, ProxyType
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from googletrans import Translator
import pandas as pd
import psycopg2

# Define your proxy IP address and port
proxy_ip = "210.179.101.88"
proxy_port = "3128"

# Configure the proxy settings
proxy = Proxy()
proxy.proxy_type = ProxyType.MANUAL
proxy.http_proxy = f"{proxy_ip}:{proxy_port}"
proxy.ssl_proxy = f"{proxy_ip}:{proxy_port}"

# Create a WebDriver with the proxy settings
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--proxy-server=http://{proxy_ip}:{proxy_port}")
driver = webdriver.Chrome(options=chrome_options)

def translate_column_names(column_names):

    
    column_mapping = {
        "अनु क्र.": "serial_number",
        "दस्त क्र.": "document_number",
        "दस्त प्रकार": "document_type",
        "दू. नि. कार्यालय": "revenue_office",
        "वर्ष": "year_year",
        "लिहून देणार": "issuer",
        "लिहून घेणार": "receiver",
        "इतर माहीती": "other_info",
        "सूची क्र. २": "list_number"
    }

    # Translate column names
    translated_column_names = [column_mapping.get(col, col) for col in column_names]
    return translated_column_names


driver = webdriver.Chrome()

# Navigate to the website
driver.get("https://pay2igr.igrmaharashtra.gov.in/eDisplay/Propertydetails/index")

#For Top year selection
dropdown_element = driver.find_element(By.ID, "dbselect")
dropdown = Select(dropdown_element)
dropdown.select_by_index(30)
time.sleep(2)

#for district selection
dropdown_element_1 = driver.find_element(By.ID, "district_id")
dropdown = Select(dropdown_element_1)
dropdown.select_by_value(str(37))
time.sleep(2)

#for Taluka selection
dropdown_element_2 = driver.find_element(By.ID, "taluka_id")
dropdown = Select(dropdown_element_2)
dropdown.select_by_value(str(1))
time.sleep(2)

#for village selection
dropdown_element_3 = driver.find_element(By.ID, "village_id")
dropdown = Select(dropdown_element_3)
dropdown.select_by_value(str(57))
time.sleep(2)

#entering year 
search = driver.find_element("name","free_text")
search.send_keys("2023")
search.send_keys(Keys.RETURN)


time.sleep(30)

table_list = []

# Extracting the table data
table = driver.find_element(By.XPATH, "//table[@id='tableparty']")

table_headers = table.find_elements(By.TAG_NAME, "th")
header_list = [header.text for header in table_headers]
translated_header_list = translate_column_names(header_list)
data_rows = []

table_rows = table.find_elements(By.TAG_NAME, "tr")[1:]  
for row in table_rows:
    row_data = [cell.text for cell in row.find_elements(By.TAG_NAME, "td")]   # Extract data from each cell in the row
    data_rows.append(row_data)

df = pd.DataFrame(data_rows, columns=translated_header_list)

driver.quit()

conn = psycopg2.connect(
    host="localhost",
    database="PropReturns",
    user="postgres",
    password="Dhyey@16",
    port=5435
)

cursor = conn.cursor()

# Define a list of all Hindi characters
hindi_characters = ['अ', 'आ', 'इ', 'ई', 'उ', 'ऊ', 'ऋ', 'ए', 'ऐ', 'ओ', 'औ', 'क', 'ख', 'ग', 'घ', 'ङ', 'च', 'छ', 'ज', 'झ', 'ञ', 'ट', 'ठ', 'ड', 'ढ', 'ण', 'त', 'थ', 'द', 'ध', 'न', 'प', 'फ', 'ब', 'भ', 'म', 'य', 'र', 'ल', 'व', 'श', 'ष', 'स', 'ह', 'ा', 'ि', 'ी', 'ु', 'ू', 'ृ', 'े', 'ै', 'ो', 'ौ', '्', 'ॐ', '।', '॥']

# Create a Table Query
table_name = "MBProp"
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("

# Iterate through DataFrame columns to define table columns and data types
for column_name, data_type in zip(df.columns, df.dtypes):
    # Map Pandas data types to PostgreSQL data types (you may need to customize this)
    postgres_data_type = "VARCHAR" if data_type == 'object' else "INTEGER"
    
    # Use double quotes around column names with non-English characters
    if any(char in column_name for char in hindi_characters):
        create_table_query += f'"{column_name}" {postgres_data_type}, '
    else:
        create_table_query += f"{column_name} {postgres_data_type}, "

# Define the primary key column
primary_key_column = "document_number"

# Add the primary key constraint to the primary key column
create_table_query += f'PRIMARY KEY ("{primary_key_column}")'

# Remove the trailing comma and space
create_table_query = create_table_query.rstrip(", ") + ")"

try:
    # Attempt to create the table
    cursor.execute(create_table_query)
    conn.commit()
    print("Table created successfully.")
     # Insert data into the table
    for index, row in df.iterrows():
        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s']*len(df.columns))})"
        cursor.execute(insert_query, tuple(row))
    
    # Commit the data insertion
    conn.commit()
    print("Data inserted successfully.")
except Exception as e:
    # Print the error message
    print(f"Error: {e}")
finally:
    # Close the connection
    conn.close()
#print(df)
