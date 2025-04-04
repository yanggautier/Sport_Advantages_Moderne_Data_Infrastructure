import requests
import os
from dotenv import load_dotenv
import time
from datetime import datetime
import pandas as pd
from sql_scripts import get_table_count, table_bulk_insert, get_table_elements


# Clé API API Distance Matrix(à définir dans votre fichier .env)
load_dotenv()  # Chargement des variables d'environnement
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Adresse de l'entreprise
COMPANY_ADDRESS = "1362 Av. des Platanes, 34970 Lattes"

# Limites maximales par mode de transport
TRANSPORT_LIMITS = {
    "Marche/running": 15000,  # 15 km en mètres
    "Vélo/Trottinette/Autres": 25000  # 25 km en mètres
}

def get_distance(origin, destination, mode="walking"):
    """
    Calcule la distance entre deux adresses en utilisant l'API Google Maps
    
    Args:
        origin (str): Adresse d'origine
        destination (str): Adresse de destination
        mode (str): Mode de transport (walking, bicycling, driving)
        
    Returns:
        int: Distance en mètres
        int: Durée en secondes
    """
    # Conversion du mode de transport pour l'API Google Maps
    api_mode = {
        "Marche/running": "walking",
        "Vélo/Trottinette/Autres": "bicycling"
    }.get(mode, "walking")
    
    # Construction de l'URL de l'API
    url = f"https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": origin,
        "destinations": destination,
        "mode": api_mode,
        "key": GOOGLE_MAPS_API_KEY
    }
    
    # Exécution de la requête
    response = requests.get(url, params=params)
    data = response.json()
    
    # Extraction des résultats
    try:
        distance = data["rows"][0]["elements"][0]["distance"]["value"]  # en mètres
        duration = data["rows"][0]["elements"][0]["duration"]["value"]  # en secondes
        return distance, duration
    except (KeyError, IndexError):
        print(f"Erreur lors du calcul de la distance pour {origin} -> {destination}")
        return None, None
    
    

def validate_commutes(employees_df):
    """
    Valide les déclarations de mode de transport des employés
    
    Args:
        employees_df (pd.DataFrame): DataFrame contenant les données des employés
                                    (doit contenir les colonnes 'id_employee', 'address', 'transport_mode')
        
    Returns:
        list: Liste contenant les validations et les distances calculées
    """
    results = []
    
    for _, employee in employees_df.iterrows():
        employee_id = employee['id_employee']
        address = employee['address']
        transport_mode = employee['transport_mode']
        
        # Vérification que le mode de transport est valide
        if transport_mode not in TRANSPORT_LIMITS:
            results.append({
                'id_employee': employee_id,
                'calculed_distance': None,
                'calculed_duration': None,
                'is_valid': False,
                'error_message': f"Mode de transport '{transport_mode}' non sportif"
            })
            continue
        
        # Calcul de la distance avec l'API Google Maps
        distance, duration = get_distance(address, COMPANY_ADDRESS, transport_mode)
        
        if distance is None:
            results.append({
                'id_employee': employee_id,
                'calculed_distance': None,
                'calculed_duration': None,
                'is_valid': False,
                'error_message': "Impossible de calculer la distance"
            })
            continue
            
        # Vérification de la distance maximale
        max_distance = TRANSPORT_LIMITS[transport_mode]
        is_valid = distance <= max_distance
        
        reason = None if is_valid else f"Distance ({distance/1000:.1f} km) > limite ({max_distance/1000} km)"
        
        results.append({
            'id_employee': employee_id,
            'calculed_distance': distance,
            'calculed_duration': duration,
            'is_valid': is_valid,
            'error_message': reason
        })
        
        # Pause pour éviter de dépasser les limites de l'API
        time.sleep(0.2)
    
    return results


def insert_validation_to_db(host, database, user, password, port):
    """
    Permet d'appeler la fonction de validation et d'insérer les données de validation dans la base de données
    
    Args:
        host: Hôte du serveur (localhost ou hôte du serveur dans Docker, par exemple: postgres)
        database: Nom de base de données
        user: Nom d'utilisateur pour connecter à la base de données
        password: Mot de passe pour établir la connection
        port: Port de connection (3306 par défaut pour MySQL, 5432 pour PostgreSQL)
    """
    # Récupérer les données qui se trouvent dans la base de données
    employees_df = get_table_elements("sport_advantages.employees", host, database, user, password, port)
    
    if employees_df is None or employees_df.empty:
        print("Aucune donnée d'employé trouvée dans la base de données")
        return
    
    # Validation des trajets
    validations = validate_commutes(employees_df)

    # Définition de liste pour bulk insert
    insert_list = []

    # Ajoute de données de validation dans la table de validation
    for row in validations:
        insert_list.append((
            row["id_employee"], 
            row["calculed_distance"], 
            row["calculed_duration"],
            row["is_valid"],
            row["error_message"]
        ))

    if insert_list:
        table_bulk_insert("sport_advantages.commute_validations", insert_list, host, database, user, password, port)
    else:
        print("Aucune validation à insérer")

def insert_employee(rh_file_path, host, database, user, password, port):
    """
    Ajout des données RH dans la base de données
    
    Args:
        rh_file_path (str): Le chemin de fichier RH
        host: Hôte du serveur
        database: Nom de base de données
        user: Nom d'utilisateur pour connecter à la base de données
        password: Mot de passe pour établir la connection
        port: Port de connection
    """
    
    # Vérifier si le fichier existe
    if not os.path.exists(rh_file_path):
        print(f"Le fichier {rh_file_path} n'existe pas")
        return
    
    # Lire les données du fichier excel RH
    employees_df = pd.read_excel(rh_file_path)

    # Récupérer ces variables pour que ça puisse mettre pour un bulk insert
    list_employees = []
    for _, employee in employees_df.iterrows():
        try:
            id_employee = employee['ID salarié']
            last_name = employee['Nom']
            first_name = employee['Prénom']
            birthday = employee['Date de naissance']
            business_unity = employee['BU']
            hire_date = employee["Date d'embauche"]
            address = employee['Adresse du domicile']
            gross_salary = employee['Salaire brut']
            transport_mode = employee['Moyen de déplacement']
            constract_type = employee['Type de contrat']
            paid_leaved_days = employee['Nombre de jours de CP']

            list_employees.append((
                id_employee, first_name, last_name, birthday, business_unity, 
                hire_date, gross_salary, constract_type, paid_leaved_days, 
                address, transport_mode
            ))
        except KeyError as e:
            print(f"Erreur lors de la lecture des données: {e}")
            continue

    # Ajout dans la base de données
    if list_employees:
        table_bulk_insert("sport_advantages.employees", list_employees, host, database, user, password, port)
    else:
        print("Aucun employé à insérer")


# Exemple d'utilisation
if __name__ == "__main__":

    # Chargement des variables d'environnement
    load_dotenv()

    # Récupérer les variables d'environnement dans des variables locals
    host = os.getenv("POSTGRES_HOST", "localhost")
    database = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    port = os.getenv("POSTGRES_PORT", "5432")
    
    # Vérifier que les variables requises sont définies
    required_vars = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "GOOGLE_MAPS_API_KEY"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Variables d'environnement manquantes: {', '.join(missing_vars)}")
        exit(1)

    # Récupérer le nombre de salarié dans la base de données
    number_employees = get_table_count("sport_advantages.employees", host, database, user, password, port)

    # S'il n'y a pas données d'employée on récupère les données dans le fichier excel pour mettre à l'intérieur
    if number_employees == 0:
        rh_file_path = "data/Données+RH.xlsx"
        print(f"Aucun employé trouvé. Insertion des données depuis {rh_file_path}")
        insert_employee(rh_file_path, host, database, user, password, port)

    # Requête pour récupérer s'il y a des données dans la table de validation
    number_validations = get_table_count("sport_advantages.commute_validations", host, database, user, password, port)
 
    # S'il n'y a pas données de validation on récupère les données des employées puis valider et mettre dans la bdd
    if number_validations == 0:
        print("Aucune validation trouvée. Exécution du processus de validation.")
        insert_validation_to_db(host, database, user, password, port)