import requests
from bs4 import BeautifulSoup
from selenium import webdriver




def getCountyPropertyId(county, formatted_apn):
  browser = webdriver.Firefox()
  url = "https://esearch."+county+"-cad.org/Search/Result?keywords=PropertyId:"+formatted_apn
  browser.get(url)
  html = browser.page_source
  soup = BeautifulSoup(html, 'lxml')
  table = soup.find('table', class_ = "k-selectable")
  browser.quit()
  return table.findChildren('td')[1].text

def getLastSaleDeed(county, county_id):
  print("here")
  url = "https://esearch."+county+"-cad.org/Property/View/"+county_id
  print("there")
  page = requests.get(url)
  print("everywhere")
  soup = BeautifulSoup(page.content, "html.parser")
  table = soup.findAll('table', class_ = "table table-striped table-bordered table-condensed")
  # Find the last table in the page, and the second row in the last table
  last_table_second_row = table[-1]('tr')[1]
  try:
    last_table_second_row_first_column = last_table_second_row.findAll('td')[0].contents[0]
  except:
    return '01/01/1990'
  print(last_table_second_row_first_column)
  return last_table_second_row_first_column if len(last_table_second_row_first_column) > 4 else '01/01/1990'


#print(getCountyPropertyId("000002-000560"))
#print(getLastSaleDeed("65"))
