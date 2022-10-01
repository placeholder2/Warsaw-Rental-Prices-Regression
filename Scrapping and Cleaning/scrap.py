"""Script for scrapping and transforming data price of rental apartments in Warsaw from otodom.pl """

from urllib.error import HTTPError
from urllib.request import Request
from urllib.request import urlopen
import csv
from bs4 import BeautifulSoup as bs
import pandas as pd
import time


# Scraping the data


# Function scrapping the data
def make_csv():
    with open('data_row.csv', 'w', encoding='UTF8', newline='') as f:
        header = ['Link', 'Cena', 'Adres', 'Stan wykończenia', 'Obsługa zdalna', 'Balkon / ogród / taras',
                  'Dostępne od', 'Rodzaj zabudowy', 'Piętro', 'Kaucja', 'Liczba pokoi', 'Czynsz',
                  'Powierzchnia', 'Typ ogłoszeniodawcy', 'Wynajmę również studentom', 'Wyposażenie',
                  'Media', 'Ogrzewanie', 'Zabezpieczenia', 'Okna', 'Winda',
                  'Miejsce parkingowe', 'Rok budowy', 'Materiał budynku', 'Informacje dodatkowe']

        writer = csv.writer(f)
        writer.writerow(header)
        r = 64
        for i in range(1, r):
            u = 'https://www.otodom.pl/pl/oferty/wynajem/mieszkanie/warszawa?distanceRadius=0&page={' \
                '}&limit=36&market=ALL&ownerTypeSingleSelect=ALL&locations=%5Bcities_6-26%5D&description=do' \
                '+zamieszkania&media=%5B%5D&extras=%5B%5D&viewType=listing '
            try:
                page = u.format(i)
                req = Request(page, headers={'User-Agent': "Mozilla/5.0"})
                sauce = urlopen(req).read()
                soup0 = bs(sauce, 'lxml')
            except HTTPError:
                time.sleep(60)
                continue

            # Inner function for finding and writing data
            def get_apt(a):
                row = []
                href = a.find('a', class_="css-rvjxyq es62z2j14")['href']
                link = ''.join(['https://www.otodom.pl', href])
                row.append(link)
                h = {'User-agent': 'Mozilla/5.0 '}
                request = Request(link, headers=h)
                s = urlopen(request).read()
                soup1 = bs(s, 'lxml')
                try:
                    cost = soup1.find('div', class_="css-1sxg93g e1t9fvcw3").find('strong')
                    row.append(cost.text)
                except AttributeError:
                    row.append('')
                try:
                    data0 = soup1.find('div', class_="css-1k12nzr eu6swcv15")
                    row.append(data0.text)
                except AttributeError:
                    row.append('')
                data1 = soup1.find('div', class_="css-wj4wb2 emxfhao1")
                for entry0 in data1.find_all('div', class_="css-1qzszy5 estckra8")[::-2]:
                    row.append(entry0.text)
                for entry1 in soup1.find_all('div', class_="css-f45csg estckra9"):
                    for entry2 in entry1.find_all('div', {"class": ["css-1wi2w6s estckra5", "css-1wnyucs estckra5"]}):
                        row.append(entry2.text)

                writer.writerow(row)

            for apartment in soup0.find_all('li', class_="css-p74l73 es62z2j17"):
                try:
                    get_apt(apartment)
                except HTTPError:
                    time.sleep(60)
                    continue

            print(f'Page {i} done')


# Transforming the data


# Function to split address into district and street
def split_addresses(df):
    s = df["Adres"].str.split(", ", n=2, expand=True)
    df['District'] = s[1]
    df['Street'] = s[2]


# Function splitting columns
def make_cols(df):
    df['Washing machine'] = df['Wyposażenie'].str.contains('pralka')
    df['Dishwasher'] = df['Wyposażenie'].str.contains('zmywarka')
    df['Furniture'] = df['Wyposażenie'].str.contains('meble')
    df['Oven'] = df['Wyposażenie'].str.contains('piekarnik')
    df['Stove'] = df['Wyposażenie'].str.contains('kuchenka')
    df['TV'] = df['Wyposażenie'].str.contains('telewizor')
    df['Refrigerator'] = df['Wyposażenie'].str.contains('lodówka')
    df['Cable TV'] = df['Media'].str.contains('telewizja kablowa')
    df['Internet'] = df['Media'].str.contains('internet')
    df['Telephone'] = df['Media'].str.contains('telefon')
    df['Security doors / windows'] = df['Informacje dodatkowe'].str.contains('drzwi / okna antywłamaniowe')
    df['Closed area'] = df['Informacje dodatkowe'].str.contains('teren zamknięty')
    df['Intercom / videophone'] = df['Informacje dodatkowe'].str.contains('wideofon, monitoring')
    df['Monitoring / security'] = df['Informacje dodatkowe'].str.contains('monitoring / ochrona')
    df['Anti-burglary blinds'] = df['Informacje dodatkowe'].str.contains('rolety antywłamaniowe')
    df['Alarm system'] = df['Informacje dodatkowe'].str.contains('system alarmowy')
    df['Only for non-smokers'] = df['Zabezpieczenia'].str.contains('Zabezpieczenia')
    df['Separate kitchen'] = df['Informacje dodatkowe'].str.contains('oddzielna kuchnia')
    df['Basement'] = df['Informacje dodatkowe'].str.contains('piwnica')
    df['Utility room'] = df['Informacje dodatkowe'].str.contains('pom. użytkowe')
    df['Two-level'] = df['Informacje dodatkowe'].str.contains('dwupoziomowe')
    df['Balcony'] = df['Balkon / ogród / taras'].str.contains('balkon')
    df['Terrace'] = df['Balkon / ogród / taras'].str.contains('taras')
    df['Garden'] = df['Balkon / ogród / taras'].str.contains('ogródek')
    df['Winda'] = df['Winda'].str.contains('tak')
    df['Miejsce parkingowe'] = df['Miejsce parkingowe'].str.contains('garaż/miejsce parkingowe')


# Function for replacing and translating rows
def rep_rows(df):
    df = df.replace('zapytaj', '')
    df = df.replace('brak informacji', '')
    df['Typ ogłoszeniodawcy'] = df['Typ ogłoszeniodawcy'].replace('biuro nieruchomości ', 'estate agency')
    df['Typ ogłoszeniodawcy'] = df['Typ ogłoszeniodawcy'].replace('prywatny', 'private')
    df['Ogrzewanie'] = df['Ogrzewanie'].replace('miejskie', 'urban')
    df['Ogrzewanie'] = df['Ogrzewanie'].replace('gazowe', 'gas')
    df['Ogrzewanie'] = df['Ogrzewanie'].replace('kotłownia', 'boiler')
    df['Okna'] = df['Okna'].replace('plastikowe', 'plastic')
    df['Okna'] = df['Okna'].replace('drewniane', 'wood')
    df['Okna'] = df['Okna'].replace('aluminiowe', 'aluminum')
    df['Materiał budynku'] = df['Materiał budynku'].replace('cegła', 'brick')
    df['Materiał budynku'] = df['Materiał budynku'].replace('inne', 'other')
    df['Materiał budynku'] = df['Materiał budynku'].replace('wielka płyta', 'great plate')
    df['Materiał budynku'] = df['Materiał budynku'].replace('silikat', 'silicate')
    df['Materiał budynku'] = df['Materiał budynku'].replace('żelbet', 'reinforced concrete')
    df['Materiał budynku'] = df['Materiał budynku'].replace('pustak', 'airbrick')
    df['Rodzaj zabudowy'] = df['Rodzaj zabudowy'].replace('blok', ' block of flats')
    df['Rodzaj zabudowy'] = df['Rodzaj zabudowy'].replace('apartamentowiec', 'apartment building')
    df['Rodzaj zabudowy'] = df['Rodzaj zabudowy'].replace('kamienica', 'tenement house')
    df['Rodzaj zabudowy'] = df['Rodzaj zabudowy'].replace('dom wolnostojący', 'detached house')
    df['Czynsz'] = df['Czynsz'].replace('\miesiąc', '')
    df['Czynsz'] = df['Czynsz'].str.split('zł/miesiąc', expand=True)[0]
    df['Kaucja'] = df['Kaucja'].str.split(" zł", n=1, expand=True)[0]
    df['Cena'] = df['Cena'].str.split(" zł", n=1, expand=True)[0]
    df['Piętro'] = df['Piętro'].str.replace('parter', '0')

    return df


# Function for replacing and translating columns
def rep_cols(df):
    df = df.drop(
        df[['Stan wykończenia', 'Dostępne od',
            'Obsługa zdalna', 'Wynajmę również studentom',
            'Balkon / ogród / taras', 'Zabezpieczenia',
            'Informacje dodatkowe', 'Wyposażenie', 'Media']],
        axis=1)

    df = df.rename(
        columns={'Cena': 'Price', 'Adres': 'Address',
                 'Rodzaj zabudowy': 'Type of building',
                 'Piętro': 'Floor', 'Kaucja': 'Deposit',
                 'Liczba pokoi': 'Bedrooms',
                 'Czynsz': 'Rent', 'Powierzchnia': 'Area',
                 'Typ ogłoszeniodawcy': 'Advertiser',
                 'Ogrzewanie': 'Heating', 'Okna': 'Windows',
                 'Winda': 'Elevator', 'Miejsce parkingowe': 'Parking space',
                 'Rok budowy': 'Year of construction',
                 'Materiał budynku': 'Building material'})
    return df


# Putting together all the data transforming functions
def transform():
    data = pd.read_csv('data_row.csv')
    split_addresses(data)
    make_cols(data)
    data = rep_rows(data)
    data = rep_cols(data)
    data.to_csv('data.csv', na_rep='(missing)')


if __name__ == "__main__":
    start = time.time()
    make_csv()
    print('Data scrapped')
    transform()
    time.sleep(2)
    print('Transformed')
    end = time.time()
    elapsed = round(end - start)
    print(f'Done in {elapsed} seconds')
