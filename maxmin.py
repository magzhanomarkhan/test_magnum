from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import pandas as pd

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

url = 'https://kurs.kz/'
driver.get(url)

try:
    wait = WebDriverWait(driver, 20)

    purchase_elements = driver.find_elements(By.CSS_SELECTOR, '.col-5.text-end.currency.svelte-sdi4lo')

    max_purchase_value = float('-inf')
    min_purchase_value = float('inf')

    for element in purchase_elements:
        try:
            value_text = element.text.strip()

            if not value_text:
                continue

            value = round(float(value_text.replace(',', '.')), 1)

            if value != 0:  
                if value > max_purchase_value:
                    max_purchase_value = value
                if value < min_purchase_value:
                    min_purchase_value = value

        except ValueError:
            continue

        except Exception as e:
            print(f'Error processing purchase element: {e}')
            continue

    sale_elements = driver.find_elements(By.CSS_SELECTOR, '.col-5.text-start.currency.svelte-sdi4lo')

    max_sale_value = float('-inf')
    min_sale_value = float('inf')

    for element in sale_elements:
        try:
            value_text = element.text.strip()

            if not value_text:
                continue

            value = round(float(value_text.replace(',', '.')), 1)

            if value != 0:  
                if value > max_sale_value:
                    max_sale_value = value
                if value < min_sale_value:
                    min_sale_value = value

        except ValueError:
            continue

        except Exception as e:
            print(f'Error processing sale element: {e}')
            continue

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if max_purchase_value != float('-inf'):
        print(f'{current_time}: Highest purchase rate found: {max_purchase_value:.1f}')
    else:
        print(f'{current_time}: No valid purchase rates found.')

    if min_purchase_value != float('inf'):
        print(f'{current_time}: Lowest purchase rate found: {min_purchase_value:.1f}')
    else:
        print(f'{current_time}: No valid purchase rates found.')

    if max_sale_value != float('-inf'):
        print(f'{current_time}: Highest sale rate found: {max_sale_value:.1f}')
    else:
        print(f'{current_time}: No valid sale rates found.')

    if min_sale_value != float('inf'):
        print(f'{current_time}: Lowest sale rate found: {min_sale_value:.1f}')
    else:
        print(f'{current_time}: No valid sale rates found.')

    results = {
        'Time': [current_time],
        'Max Purchase Rate': [max_purchase_value if max_purchase_value != float('-inf') else None],
        'Min Purchase Rate': [min_purchase_value if min_purchase_value != float('inf') else None],
        'Max Sale Rate': [max_sale_value if max_sale_value != float('-inf') else None],
        'Min Sale Rate': [min_sale_value if min_sale_value != float('inf') else None]
    }

    df = pd.DataFrame(results)

    excel_file = 'currency_rates.xlsx'
    df.to_excel(excel_file, index=False)

    print(f'Data saved to {excel_file}')

finally:
    driver.quit()
