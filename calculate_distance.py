import pandas as pd
import requests
import os
from dotenv import load_dotenv
import time
from datetime import datetime

# Chargement des variables d'environnement
load_dotenv()

# Clé API API Distance Matrix(à définir dans votre fichier .env)
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
                                    (doit contenir les colonnes 'id', 'adresse', 'mode_transport')
        
    Returns:
        pd.DataFrame: DataFrame contenant les validations et les distances calculées
    """
    results = []
    
    for _, employee in employees_df.iterrows():
        employee_id = employee['ID salarié']
        address = employee['Adresse du domicile']
        transport_mode = employee['Moyen de déplacement']
        
        # Vérification que le mode de transport est valide
        if transport_mode not in TRANSPORT_LIMITS:
            results.append({
                'id_salarie': employee_id,
                'adresse': address,
                'mode_transport': transport_mode,
                'distance_calculee': None,
                'duree_calculee': None,
                'est_valide': False,
                'raison': f"Mode de transport '{transport_mode}' non sportif"
            })
            continue
        
        # Calcul de la distance avec l'API Google Maps
        distance, duration = get_distance(address, COMPANY_ADDRESS, transport_mode)
        
        if distance is None:
            results.append({
                'id_salarie': employee_id,
                'adresse': address,
                'mode_transport': transport_mode,
                'distance_calculee': None,
                'duree_calculee': None,
                'est_valide': False,
                'raison': "Impossible de calculer la distance"
            })
            continue
            
        # Vérification de la distance maximale
        max_distance = TRANSPORT_LIMITS[transport_mode]
        is_valid = distance <= max_distance
        
        reason = None if is_valid else f"Distance ({distance/1000:.1f} km) > limite ({max_distance/1000} km)"
        
        results.append({
            'id_salarie': employee_id,
            'adresse': address,
            'mode_transport': transport_mode,
            'distance_calculee': distance,
            'duree_calculee': duration,
            'est_valide': is_valid,
            'raison': reason
        })
        
        # Pause pour éviter de dépasser les limites de l'API
        time.sleep(0.2)
    
    return pd.DataFrame(results)
# Exemple d'utilisation
if __name__ == "__main__":
    # Exemple avec un DataFrame d'employés fictif
    rh_df = pd.read_excel("data/Données+RH.xlsx")
    
    # Validation des trajets
    validations = validate_commutes(rh_df)
    
    # Affichage des résultats
    print(validations[['id_salarie', 'mode_transport', 'distance_calculee', 'est_valide', 'raison']])
    
    # Enregistrement des résultats
    validations.to_csv("data/validations_trajets.csv", index=False)