import psycopg2
from psycopg2.extras import execute_values
from pandas import DataFrame

def get_table_count(table_name, host, database, user, password, port):
    """
    Permet de compter le nombre de ligne dans la table donnée
    
    Args:
        table_name: Nom de table
        host: Hôte du serveur (localhost ou hôte du serveur dans Docker, par exemple: postgres)
        database: Nom de base de données
        user: Nom d'utilisateur pour connecter à la base de données
        password: Mot de passe pour établir la connection
        port: Port de connection (3306 par défaut pour MySQL, 5432 pour PostgreSQL)
    
    Returns:
        result: Le compte de nombre de ligne qui se trouve dans la table donnée
    """

    db_params = {"host": host, "database": database, "user": user, "password": password, "port": port}
    connection = None

    try:
        # Etablir la connection à la base de données
        connection = psycopg2.connect(**db_params)
        
        # Curseur de la connection
        cursor = connection.cursor()

        # Requête pour récupérer s'il y a des données dans la table de validation
        select_query = f"SELECT COUNT(*) FROM {table_name}"

        # Executing the SELECT query
        cursor.execute(select_query)

        # Fetching and printing the results
        result = cursor.fetchone()

        return result[0]

    except (Exception, psycopg2.Error) as error:
        print(f"Erreur de connection à la base de données: {error}")
        return 0

    finally:
        # Fermer la connection dans tout les cas
        if connection:
            cursor.close()
            connection.close()
            print("La connection à la base de données a bien été fermée.")
    

def get_table_elements(table_name, host, database, user, password, port):
    """
    Permet de récupérer tous les éléments d'une table donnée
    
    Args:
        table_name: Nom de table
        host: Hôte du serveur (localhost ou hôte du serveur dans Docker, par exemple: postgres)
        database: Nom de base de données
        user: Nom d'utilisateur pour connecter à la base de données
        password: Mot de passe pour établir la connection
        port: Port de connection (3306 par défaut pour MySQL, 5432 pour PostgreSQL)
    
    Returns:
        df: DataFrame contenant les données de la table
    """

    db_params = {"host": host, "database": database, "user": user, "password": password, "port": port}
    connection = None
    df = None

    try:
        # Etablir la connection à la base de données
        connection = psycopg2.connect(**db_params)
        
        # Curseur de la connection
        cursor = connection.cursor()

        # Requête pour récupérer s'il y a des données dans la table de validation
        select_query = f"SELECT * FROM {table_name}"

        # Exécuter la requête SELECT
        cursor.execute(select_query)

        # Récupérer les données des requêtes
        df = DataFrame(cursor.fetchall())
        if not df.empty:
            df.columns = [desc[0] for desc in cursor.description]
    except (Exception, psycopg2.Error) as error:
        print(f"Erreur de connection à la base de données: {error}")

    finally:
         # Fermer la connection dans tout les cas
         if connection:
            cursor.close()
            connection.close()
            print("La connection à la base de données a bien été fermée.")
            
    return df


def table_bulk_insert(table_name, list_data, host, database, user, password, port):
    """
    Args:
        table_name: Nom de table à insérer des données
        list_data: Liste de données à insérer
        host: Hôte du serveur (localhost ou hôte du serveur dans Docker, par exemple: postgres)
        database: Nom de base de données
        user: Nom d'utilisateur pour connecter à la base de données
        password: Mot de passe pour établir la connection
        port: Port de connection (3306 par défaut pour MySQL, 5432 pour PostgreSQL)
    """

    db_params = {"host": host, "database": database, "user": user, "password": password, "port": port}
    connection = None

    try:
        # Etablir la connection à la base de données
        connection = psycopg2.connect(**db_params)
        
        # Curseur de la connection
        cursor = connection.cursor()

        # Requête à exécuter
        if table_name == "sport_advantages.commute_validations": 
            columns = "(id_employee, calculed_distance, calculed_duration, is_valid, error_message)"
            query = f"INSERT INTO {table_name} {columns} VALUES %s"
        elif table_name == "sport_advantages.employees":
            columns = "(id_employee, first_name, last_name, birthday, business_unity, hire_date, gross_salary, constract_type, paid_leaved_days, address, transport_mode)"
            query = f"INSERT INTO {table_name} {columns} VALUES %s"
        else:
            raise ValueError("Nom de table incorrect")

        # Exécuter la requête
        execute_values(cursor, query, list_data)
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print(f"Erreur de connection à la base de données: {error}")

    finally:
         # Fermer la connection dans tout les cas
         if connection:
            cursor.close()
            connection.close()
            print("La connection à la base de données a bien été fermée.")