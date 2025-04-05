import psycopg2
from typing import List, Tuple
from psycopg2.extras import execute_values


def get_employee_ids(host:str, database:str, user:str, password:str, port:int) -> List[int]:
    """
    Récupérer la liste des identifiants des salariés
    
    Args:
        host: Hôte du serveur (localhost ou hôte du serveur dans Docker, par exemple: postgres)
        database: Nom de base de données
        user: Nom d'utilisateur pour connecter à la base de données
        password: Mot de passe pour établir la connection
        port: Port de connection (3306 par défaut pour MySQL, 5432 pour PostgreSQL)
    
    Returns:
        liste des id (List[int]): Liste des identifiants des salariés
    """
    try:
        db_params = {"host": host, "database": database, "user": user, "password": password, "port": port}
        
        # Etablir la connection à la base de données
        connection = psycopg2.connect(**db_params)

        # Curseur de la connection
        cursor = connection.cursor()

        # Requête pour récupérer s'il y a des données dans la table de validation
        select_query = "SELECT id_employee FROM sport_advantages.employees"

        # Exécuter la requête SELECT
        cursor.execute(select_query)

        # Récupérer les données des requêtes
        result = cursor.fetchall()

        return [row[0] for row in result]

    except (Exception, psycopg2.Error) as error:
        print(f"Erreur de connection à la base de données: {error}")

    finally:
         # Fermer la connection dans tout les cas
         if connection:
            cursor.close()
            connection.close()
            print("La connection à la base de données a bien été fermée.")
        

def get_activitis_nb(host:str, database:str, user:str, password:str, port:int) -> List[int]:
    """
    Récupérer la liste des identifiants des salariés
    
    Args:
        host: Hôte du serveur (localhost ou hôte du serveur dans Docker, par exemple: postgres)
        database: Nom de base de données
        user: Nom d'utilisateur pour connecter à la base de données
        password: Mot de passe pour établir la connection
        port: Port de connection (3306 par défaut pour MySQL, 5432 pour PostgreSQL)
    
    Returns:
        liste des id (List[int]): Liste des identifiants des salariés
    """
    try:
        db_params = {"host": host, "database": database, "user": user, "password": password, "port": port}
        
        # Etablir la connection à la base de données
        connection = psycopg2.connect(**db_params)

        # Curseur de la connection
        cursor = connection.cursor()

        # Requête pour récupérer s'il y a des données dans la table de validation
        select_query = "SELECT COUNT(*) FROM sport_advantages.sport_activities"

        # Exécuter la requête SELECT
        cursor.execute(select_query)

        # Récupérer le résultat
        result = cursor.fetchone()

        return result[0]

    except (Exception, psycopg2.Error) as error:
        print(f"Erreur de connection à la base de données: {error}")

    finally:
         # Fermer la connection dans tout les cas
         if connection:
            cursor.close()
            connection.close()
            print("La connection à la base de données a bien été fermée.")
        


def bulk_insert_sport_activities(list_data: List[Tuple], host: str, database:str, user: str, password: str, port: int):
    """
    Args:
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
        columns = "(id_employee, start_datetime, sport_type, distance, activity_duration, comment)"
        query = f"INSERT INTO sport_advantages.sport_activities {columns} VALUES %s"

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