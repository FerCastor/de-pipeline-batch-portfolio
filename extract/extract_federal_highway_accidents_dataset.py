

def extract_from_kaggle(dataset):
    import kagglehub
    import os
    import shutil
    from pathlib import Path

    dataset_path = kagglehub.dataset_download(dataset)
    print(f"Dataset downloaded to: {dataset_path}")

    # Lista arquivos no diretório
    files = os.listdir(dataset_path)
    print("Files in dataset:", files)

    # Retorna o caminho completo do arquivo CSV
    csv_file = os.path.join(dataset_path, files[0])

    # Cria pasta se não existir
    # Path("data/bronze").mkdir(parents=True, exist_ok=True)
 
    shutil.copy(csv_file, "/home/fernanda/Documentos/portfolio/de-pipeline-batch-portfolio/data/bronze/datatran.csv")
    return csv_file


def read_csv_from_kaggle(dataset_path):
    import pandas as pd
    df = pd.read_csv(dataset_path)
    return df






if __name__ == "__main__":
    csv_file_path = extract_from_kaggle("alinebertolani/federal-highway-accidents-dataset")
    print(f"CSV file path: {csv_file_path}")
    df = read_csv_from_kaggle(csv_file_path)
    print(df.columns)
    print(df.head(1))
