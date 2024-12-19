import pandas as pd
import os
import glob


def extrai_dados_e_consolida(path:str) ->pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(path,'*.json'))
    df_list =[pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list,ignore_index=True)
    return df_total

def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"]*df["Venda"]
    return df

def carrega_os_dados_para_csv(df: pd.DataFrame,list_type:str):
    for type in list_type:
        if type == 'csv':
           df.to_csv("dados.csv",index=False,header=True)
        if type == 'parquet':
            df.to_parquet("dados.parquet")

def pipeline_calcula_kpi_vendas_consolidado(path:str,type:list):
    pasta = path
    df= extrai_dados_e_consolida(path=pasta)
    df = calcular_kpi_de_total_de_vendas(df)
    carrega_os_dados_para_csv(df,type)

if __name__ == "__main__":
    pasta = 'data'
    df= extrai_dados_e_consolida(path=pasta)
    df = calcular_kpi_de_total_de_vendas(df)
    carrega_os_dados_para_csv(df,["parquet","csv"])