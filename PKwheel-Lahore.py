from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import pyodbc
import time
import pandas as pd

# User-Agent Header for Web Scraping
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
}

# Scrape PakWheels listings
def scrape_pakwheels():
    BetterDF = []
    
    # Set up Chrome WebDriver using WebDriver Manager
    options = webdriver.ChromeOptions()
    options.add_argument(f"user-agent={headers['User-Agent']}")
    options.add_argument('--ignore-certificate-errors') # Make Ignore is driver / webchrome old verion error SSL
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    

    # Loop through multiple pages
    for i in range(1, 391):  # Modify range as needed
        print(f'Page number is {i}')
        url = f'https://www.pakwheels.com/used-cars/lahore/24858?registration_city=punjab&page={i}'
        driver.get(url)
        time.sleep(3)  # Add delay for the page to load
        
        # Find all car listing containers
        containers = driver.find_elements(By.CLASS_NAME, 'col-md-9.grid-style')

        for container in containers:
            # Extract title (car name)
            title = container.find_element(By.TAG_NAME, 'h3').text.strip()

            # Extract price
            price = container.find_element(By.CLASS_NAME, 'price-details').text.strip().replace('\n', '')

            # Extract vehicle info
            ul = container.find_element(By.CLASS_NAME, 'list-unstyled.search-vehicle-info-2.fs13')
            li = ul.find_elements(By.TAG_NAME, 'li')
            Year = li[0].text.strip()
            Mileage = li[1].text.strip()
            Fuel = li[2].text.strip()
            HP = li[3].text.strip()
            Transmission = li[4].text.strip()

            # Extract link
            link = container.find_element(By.CLASS_NAME, 'car-name.ad-detail-path').get_attribute('href')

            page = 'page ' + str(i)

            BetterDF.append([title, price, Year, Mileage, Fuel, HP, Transmission, page, link])  # Store scraped data

    driver.quit()  # Close the browser

    # Convert data to DataFrame
    df = pd.DataFrame(BetterDF, columns=['TITLE', 'PRICE',  'Year', 'Mileage', 'Fuel Type', 'Horsepower', 'Transmission', 'Page', 'Link'])
    
    return df

# Insert scraped data into SQL Server
def insert_into_database(cars_df):
    # SQL Server connection setup with Windows Authentication
    connection = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=DESKTOP-J2KJLA0\\SQLEXPRESS;'  # My SQL Server name
        'DATABASE=FinalProject1;'  # My SQL database name
        'Trusted_Connection=yes;'
    )
    
    cursor = connection.cursor()

    # Create table if it doesn't exist
    cursor.execute(''' 
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='PakWheels_Cars_Lhr' AND xtype='U')
    BEGIN
        CREATE TABLE PakWheels_Cars_Lhr (
            id INT IDENTITY(1,1) PRIMARY KEY,
            Title NVARCHAR(255) NOT NULL,
            Price NVARCHAR(50),
            Year NVARCHAR(10),
            -- Rating NVARCHHAR(10),
            Mileage NVARCHAR(50),
            Fuel_Type NVARCHAR(50),
            Horsepower NVARCHAR(50),
            Transmission NVARCHAR(50),
            Page NVARCHAR(10),
            Link NVARCHAR(MAX)
        )
    END
    ''')

    # Insert data into the table
    for index, row in cars_df.iterrows():
        cursor.execute('''
            INSERT INTO PakWheels_Cars_Lhr (Title, Price, Year, Mileage, Fuel_Type, Horsepower, Transmission, Page, Link)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', 
        row['TITLE'], 
        row['PRICE'],
        row['Year'], 
        row['Mileage'], 
        row['Fuel Type'], 
        row['Horsepower'], 
        row['Transmission'], 
        row['Page'], 
        row['Link']
    )
    
    connection.commit()
    cursor.close()
    connection.close()

# Main function to run scraping and insert data into the database
if __name__ == '__main__':
    cars_df = scrape_pakwheels()  # Scrape the PakWheels website
    insert_into_database(cars_df)  # Insert the scraped data into the SQL Server
    print("Scraping and database insertion completed successfully.")
