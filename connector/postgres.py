
def connecta_postgres():
    import psycopg2
    import os
    from dotenv import load_dotenv

    load_dotenv()
    connection = None

    try:
        connection = psycopg2.connect(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host="127.0.0.1",
            port="5432",
            database=os.getenv("POSTGRES_DB")
        )
        cursor = connection.cursor()

        # Exemplo de consulta
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("Conectado ao - ", record)

    except Exception as error:
        print("Erro ao conectar ao PostgreSQL", error)

    finally:
        # Fechando a conexão
        if connection is not None:
            cursor.close()
            connection.close()
            print("PostgreSQL connection closed")

connecta_postgres()