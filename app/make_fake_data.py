import argparse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from faker import Faker
import time
from datetime import datetime
from dotenv import load_dotenv, dotenv_values
from io import StringIO
import boto3
import json

# função para parsear a saída do parâmetro SILENT
def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

# Instancia a classe Faker
faker = Faker()

# Função MAIN
if __name__ == "__main__":

    data_lake = 'local'  # local - s3
    
    parser = argparse.ArgumentParser(description='Generate fake data...')

    parser.add_argument('--interval', type=int, default=0.5,
                        help='interval of generating fake data in seconds')
    parser.add_argument('-n', type=int, default=1,
                        help='sample size')
    parser.add_argument('--silent', type=str2bool, nargs='?',
                        const=True, default=False,
                        help="print fake data")

    args = parser.parse_args()

    print(f"Args parsed:")
    print(f"Interval: {args.interval}")
    print(f"Sample size; {args.n}")
    print(f"Data Lake; {data_lake}")
    
    # pegando credencias da AWS
    AWS_SECRET_ACCESS_KEY = dotenv_values('.env')['AWS_SECRET_ACCESS_KEY']
    AWS_ACCESS_KEY_ID     = dotenv_values('.env')['AWS_ACCESS_KEY_ID']  
    REGION_NAME           = dotenv_values('.env')['REGION_NAME'] 
    BUCKET                = 'databricks-spark-streaming'   

    #-----------------------------------------------------------------
    print("Iniciando a simulacao...", end="\n\n")

    qtde = 0
    dados = []

    # Gera dados fake a faz ingestáo
    while True:
        nome       = faker.name()
        gender     = np.random.choice(["M", "F"], p=[0.5, 0.5])
        endereco   = faker.address()
        telefone   = faker.phone_number() 
        email      = faker.safe_email() 
        foto       = faker.image_url() 
        nascimento = faker.date_of_birth() 
        profissao  = faker.job() 
        dt_update  = datetime.now()

        dados.append({
            "nome": nome,
            "sexo": gender,
            "endereco": endereco,
            "telefone": telefone,
            "email": email,
            "foto": foto,
            "nascimento": nascimento,
            "profissao": profissao,
            "dt_update": dt_update
        })
        
        qtde += 1

        if qtde == 10:
            
            df = pd.DataFrame(dados)       
                                  
            if data_lake == 'local':
                destination = "./data-lake/landing/output_" + str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + '.json'
                df.to_json(destination, orient="records")        

            if data_lake == 's3':
                # grava no S3
                destination = "output_" + str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + '.json'

                s3 = boto3.client("s3",\
                                region_name=REGION_NAME,\
                                aws_access_key_id=AWS_ACCESS_KEY_ID,\
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                json_buf = StringIO()
                df.to_json(json_buf, orient="records")
                json_buf.seek(0)
                s3.put_object(Bucket=BUCKET, Body=json_buf.getvalue(), Key='landing/'+destination)
            
            if not args.silent:
                print(df, end="\n\n")
            
            qtde = 0 
            dados = []

        time.sleep(args.interval)