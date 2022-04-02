from selenium import webdriver
import re
import time

booking_bcn_hotel_url = "https://www.booking.com/searchresults.en-gb.html?label=barcelona-ehWEA6MN2BfWHmP5ftaRkgS541011140089%3Apl%3Ata%3Ap1%3Ap2%3Aac%3Aap%3Aneg%3Afi%3Atiaud-297601666555%3Akwd-123261865%3Alp1005424%3Ali%3Adec%3Adm%3Appccp%3DUmFuZG9tSVYkc2RlIyh9YfqnDqqG8nt10AsofPfvtt0&sid=87fb285a830e29d1d08c3b9a8db0477e&aid=1610684&lang=en-gb&sb=1&sb_lp=1&src=city&src_elem=sb&error_url=https%3A%2F%2Fwww.booking.com%2Fcity%2Fes%2Fbarcelona.en-gb.html%3Faid%3D1610684%3Blabel%3Dbarcelona-ehWEA6MN2BfWHmP5ftaRkgS541011140089%253Apl%253Ata%253Ap1%253Ap2%253Aac%253Aap%253Aneg%253Afi%253Atiaud-297601666555%253Akwd-123261865%253Alp1005424%253Ali%253Adec%253Adm%253Appccp%253DUmFuZG9tSVYkc2RlIyh9YfqnDqqG8nt10AsofPfvtt0%3Bsid%3D87fb285a830e29d1d08c3b9a8db0477e%3Binac%3D0%26%3B&ss=Barcelona&is_ski_area=0&ssne=Barcelona&ssne_untouched=Barcelona&city=-372490&checkin_year=2022&checkin_month=5&checkin_monthday=1&checkout_year=2022&checkout_month=5&checkout_monthday=2&group_adults=1&group_children=0&no_rooms=1&b_h4u_keep_filters=&from_sf=1&offset=0"
#driver = webdriver.Chrome()
driver = webdriver.Chrome("/Users/zaynluo/Downloads/chromedriver")
driver.get(booking_bcn_hotel_url)

def collect_pages(driver):
    page_source = driver.page_source
    pages = re.findall(r'https://www\.booking\.com/hotel/es/.*?\.en-gb\.html\?label=barcelona', page_source)
    return list(set([i[:-16] for i in pages]))

res = []
for i in range(30):
    res += collect_pages(driver)
    btn = driver.find_elements_by_xpath("//button[@aria-label='Next page']")[0]
    btn.click()
    time.sleep(3)

res += collect_pages(driver)

with open("bcn_hotel_url_list.txt", 'w') as f:
    f.write('\n'.join(res))



