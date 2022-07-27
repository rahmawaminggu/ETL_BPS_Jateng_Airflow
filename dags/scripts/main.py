import requests
import pandas as pd
import sys
import pyodbc as odbc

#list penduduk berdasarkan jenis kelamin
url = 'https://webapi.bps.go.id/v1/api/list/model/data/domain/3300/var/87/key/206dc477af8073658139f101fc153b32/'

def fetch_api(url):
    z = requests.get(url).json()
    return z

def dim_gender(z):
    turvar = pd.DataFrame(z['turvar'])
    conn = odbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=147.139.209.176;DATABASE=NBA_External_Analytics;UID=sa;PWD=Nb4sqlserver@')
    cursor = conn.cursor()
    cursor.execute("delete from bps_dim_gender")

    for index, row in turvar.iterrows():
        cursor.execute("""insert into bps_dim_gender
          values(?,?)
        """,row.val
	        ,row.label)
    conn.commit()
    cursor.close()

def dim_kota_kab(z):
    vervar = pd.DataFrame(z['vervar'])
    vervar = vervar.drop(vervar[(vervar.val == 3300 )].index)
    vervar[['jenis_kota_kab','desk_kota_kab']] = vervar['label'].str.split(" ",expand=True)
    conn = odbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=147.139.209.176;DATABASE=NBA_External_Analytics;UID=sa;PWD=Nb4sqlserver@')
    cursor = conn.cursor()
    cursor.execute("delete from bps_dim_kota_kab")

    for index, row in vervar.iterrows():
        cursor.execute("""insert into bps_dim_kota_kab(id_kota_kab,jenis_kota_kab,desk_kota_kab)
          values(?,?,?)
        """,row.val
	        ,row.jenis_kota_kab
          ,row.desk_kota_kab)
    conn.commit()
    cursor.close()

def dim_tahun(z):
    tahun = pd.DataFrame(z['tahun'])
    conn = odbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=147.139.209.176;DATABASE=NBA_External_Analytics;UID=sa;PWD=Nb4sqlserver@')
    cursor = conn.cursor()
    cursor.execute("delete from bps_dim_tahun")

    for index, row in tahun.iterrows():
        cursor.execute("""insert into bps_dim_tahun
          values(?,?)
        """,row.val
	        ,row.label)
    conn.commit()
    cursor.close()

def fact_table(z):
    datacontent = pd.DataFrame(z['datacontent'], index=[0]).transpose().reset_index()
    datacontent.rename(columns={"index":"id", 0:"jumlah_penduduk"},inplace=True)
    kota_kab = datacontent['id'].str.slice(start=0,stop=4)
    jenis_kelamin = datacontent['id'].str.slice(start=6,stop=8)
    tahun = datacontent['id'].str.slice(start=8,stop=-1)
    datacontent = datacontent.assign(kota_kab = kota_kab, jenis_kelamin=jenis_kelamin,tahun=tahun)
    conn = odbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=147.139.209.176;DATABASE=NBA_External_Analytics;UID=sa;PWD=Nb4sqlserver@')
    cursor = conn.cursor()
    cursor.execute("delete from bps_fact_jumlah_penduduk")

    for index, row in datacontent.iterrows():
        cursor.execute("""insert into bps_fact_jumlah_penduduk
          values(?,?,?,?,?)
        """,row.id
	        ,row.jumlah_penduduk
          ,row.kota_kab
          ,row.jenis_kelamin
          ,row.tahun)
    conn.commit()
    cursor.close()

def main(url):
    z = fetch_api(url)
    dim_gender(z)
    dim_kota_kab(z)
    dim_tahun(z)
    fact_table(z)

if __name__ == '__main__':
    main()