import csv
import requests

def main():
    truck_sales_url = 'http://api.bcb.gov.br/dados/serie/bcdata.sgs.7386/dados?formato=json'
    truck_production_url = 'http://api.bcb.gov.br/dados/serie/bcdata.sgs.1375/dados?formato=json'
    # year start in 1997
    # export_url = f'http://www.mdic.gov.br/balanca/bd/comexstat-bd/ncm/EXP_{year}.csv'
    # import_url = f'http://www.mdic.gov.br/balanca/bd/comexstat-bd/ncm/IMP_{year}.csv'
    export_total_validation = 'http://www.mdic.gov.br/balanca/bd/comexstat-bd/ncm/EXP_TOTAIS_CONFERENCIA.csv'
    import_total_validation = 'http://www.mdic.gov.br/balanca/bd/comexstat-bd/ncm/IMP_TOTAIS_CONFERENCIA.csv'
    ncm = 'http://www.mdic.gov.br/balanca/bd/tabelas/NCM.csv'
    sh = 'http://www.mdic.gov.br/balanca/bd/tabelas/NCM_SH.csv'
    country = 'http://www.mdic.gov.br/balanca/bd/tabelas/PAIS.csv'
    block = 'http://www.mdic.gov.br/balanca/bd/tabelas/PAIS_BLOCO.csv'
    state = 'http://www.mdic.gov.br/balanca/bd/tabelas/UF.csv'
    via = 'http://www.mdic.gov.br/balanca/bd/tabelas/VIA.csv'
    urf = 'http://www.mdic.gov.br/balanca/bd/tabelas/URF.csv'

    with requests.Session() as session:
        download = session.get(urf)
        content = download.content.decode('latin1')

        with open('urf.csv', 'w', encoding='utf-8') as output:
            output.write(content)

if __name__ == '__main__':
    main()
