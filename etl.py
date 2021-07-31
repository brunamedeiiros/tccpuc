# Script para etl
import pandas
import requests
from bs4 import BeautifulSoup
import mysql.connector

DB_USER = 'root'
DB_PASS = 'root'
DB_HOST = '127.0.0.1'
DB_DATABASE = 'inss'
URL_INSS = 'https://dados.gov.br/dataset/inss-beneficios-concedidos'

def factory_conexao_db():
    cnx = mysql.connector.connect(user=DB_USER, password=DB_PASS,
                                  host=DB_HOST,
                                  database=DB_DATABASE)
    return cnx


def executar_select(query):
    cnx = factory_conexao_db()
    cursor = cnx.cursor()
    cursor.execute(query)

    for a in cursor:
        print(a)

    cursor.close()
    cnx.close()


def baixar_arquivo(url):
    local_filename = url.split('/')[-1]
    r = requests.get(url, verify=False)
    f = open(local_filename, 'wb')
    for chunk in r.iter_content(chunk_size=512 * 1024):
        if chunk:  # filter out keep-alive new chunks
            f.write(chunk)
    f.close()
    return


def baixarcsvinss():
    page = requests.get(URL_INSS)
    soup = BeautifulSoup(page.text, 'html.parser')
    links = soup.find_all('a', attrs={"class": "resource-url-analytics"})

    for l in links:
        csv = l.attrs['href']
        csv = csv.split('url=')[1]
        print(f""" Baixando {csv} """)
        baixar_arquivo(csv)
        print(f""" Sucesso ao baixar {csv} """)


if __name__ == '__main__':
    print('Iniciando')
    query = "SELECT * FROM teste"
    baixarcsvinss()

    executar_select(query)
