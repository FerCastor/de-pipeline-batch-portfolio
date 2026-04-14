class PostgresLoader:
    def __init__(self, schema):
        self.schema = schema

    def connect_postgres(self):
        import psycopg2
        import os
        from dotenv import load_dotenv

        load_dotenv()
        connection = None

        try:
            connection = psycopg2.connect(
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
                database=os.getenv("POSTGRES_DB")
            )
            return connection

        except Exception as error:
            print("Erro ao conectar ao PostgreSQL", error)

        


    def get_postgres_type(self, pandas_dtype, column_name):
        col = column_name.lower()
    
        # 1. Specifics date and time
        if col == 'data_inversa':
            return 'DATE'
        if col == 'horario':
            return 'TIME'
        
        # 2. Fields with specifics lenght to varchar
        varchar_mapping = {
            'uf': 'VARCHAR(2)',           # State: BA, SP, RJ...
            'dia_semana': 'VARCHAR(20)',   # Monday, Thursday...
            'municipio': 'VARCHAR(100)', # Cities names
            'fase_dia': 'VARCHAR(50)',
            'sentido_via': 'VARCHAR(50)',
            'tipo_pista': 'VARCHAR(50)',
            'tracado_via': 'VARCHAR(50)',
            'uso_solo': 'VARCHAR(50)',
            'condicao_metereologica': 'VARCHAR(50)',
            'tipo_acidente': 'VARCHAR(100)',
            'classificacao_acidente': 'VARCHAR(100)',
            'causa_acidente': 'VARCHAR(200)'  # Long descriptions
        }
        
        if col in varchar_mapping:
            return varchar_mapping[col]
        
        # 3. Fallback with dtype to reimaning types
        dtype_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'object': 'VARCHAR(255)',  # Generic for not mapped texts
            'bool': 'BOOLEAN'
        }
        return dtype_mapping.get(str(pandas_dtype).lower(), 'VARCHAR(255)')
    

    def create_table_if_not_exists(self, schema, table_name, columns):
        connection = self.connect_postgres()
        cursor = None
        try:
            cursor = connection.cursor()
            
            cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table_name}')")
            result = cursor.fetchone()
            if result[0]:
                print(f"Table {table_name} already exists")
            else:
                print(f"Table {table_name} does not exist")
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({columns})")
                connection.commit()
        except Exception as error:
            print("Erro ao criar a tabela", error)
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
            


    def load_data_to_bronze(self, df, table_name):
        import psycopg2
        import os
        from dotenv import load_dotenv

        load_dotenv()
        connection = self.connect_postgres()
        cursor = None

        try:
            # Is hardcoded BRONZE because is the only schema to be truncated
            cursor = connection.cursor()
            placeholders = ','.join(['%s'] * len(df.columns))
            cursor.execute(f"TRUNCATE TABLE BRONZE.{table_name}") 
            print(f"Table BRONZE.{table_name} truncated")

            for i, row in df.iterrows():
                values = tuple(row.values)
                cursor.execute(f"INSERT INTO BRONZE.{table_name} VALUES ({placeholders})", values)

            connection.commit()

        except Exception as error:
            print("Erro ao carregar os dados para a camada bronze da tabela {table_name}", error)
            connection.rollback()

        finally:
            if cursor is not None:
                cursor.close()
                print("Cursor closed")
            if connection is not None:
                connection.close()
                print("PostgreSQL connection closed")

