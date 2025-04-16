import os
import random
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from sql_manipulation import get_employee_ids, bulk_insert_sport_activities, get_activitis_nb


DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')


SPORT_TYPES = [
    "Course à pied", "Marche", "Vélo", "Natation", "Randonnée", "Trottinette",
    "Escalade", "Tennis", "Yoga", "Musculation", "Corde à sauter","Badminton", 
    "Tennis de table", "Triathlon", "Équitation", "Voile", "Football", 
    "Basketball", "Judo", "Box", "Rugby"
]


COMMENTS = [
    "Super séance aujourd'hui !",
    "Je me sens en pleine forme !",
    "Nouveau record personnel !",
    "Belle journée pour faire du sport !",
    "J'adore ce parcours !",
    "Reprise du sport :)",
    "Session intense mais gratifiante",
    "Un peu difficile mais ça fait du bien",
    "Parfait pour se vider la tête après le travail",
    "Je progresse chaque jour !",
    "Belle découverte de ce nouveau parcours",
    "Je vous recommande cet endroit, c'est magnifique",
]  + [None for _ in range(30)]


def get_distance(sport_type:str) -> Optional[int]:
    """
    Permet de calculer la distance aléatoire en fonction de type de sport
    Args:
        sport_type(str): Type de sport
    Return:
        distance(int):  La distance parcourue en mètre
    """

    if sport_type in ["Course à pied", "Marche", "Randonnée"]:
        # Distance en mètres
        if sport_type == "Course à pied":
            distance = random.randint(3000, 15000)
        elif sport_type == "Marche":
            distance = random.randint(2000, 8000)
        else: 
            distance = random.randint(5000, 20000)

    elif sport_type in ["Vélo", "Trottinette"]:
        # Plus longue distance pour ces sports
        if sport_type == "Vélo":
            distance = random.randint(10000, 50000)
        else:  # Trottinette
            distance = random.randint(5000, 15000)
    elif sport_type == "Natation":
        distance = random.randint(500, 3000)
    else:
        # Pour les autres sports qui ne déplacent pas
        distance = None
    
    return distance


def get_duration(sport_type:str, distance: Optional[int]) -> int:
    """
    Permet de calculer la durée en fonction de type de sport ou générer une durée aléatoire
    Args:
        sport_type(str): Type de sport
        distance: Distance paroucue
    Return:
        duration(int):  La distance parcourue en seconde
    """
    
    if distance is not None:
        if sport_type == "Course à pied":
            # Vitesse moyenne entre 8 et 12 km/h
            speed = random.uniform(2.2, 3.3)  # mètres par seconde
        elif sport_type == "Marche":
            # Vitesse moyenne entre 4 et 6 km/h
            speed = random.uniform(1.1, 1.7)  # mètres par seconde
        elif sport_type == "Randonnée":
            # Vitesse moyenne entre 3 et 5 km/h
            speed = random.uniform(0.8, 1.4)  # mètres par seconde
        elif sport_type == "Vélo":
            # Vitesse moyenne entre 15 et 25 km/h
            speed = random.uniform(4.2, 6.9)  # mètres par seconde
        elif sport_type == "Trottinette":
            # Vitesse moyenne entre 10 et 15 km/h
            speed = random.uniform(2.8, 4.2)  # mètres par seconde
        elif sport_type == "Natation":
            # Vitesse moyenne entre 2 et 4 km/h
            speed = random.uniform(0.6, 1.1)  # mètres par seconde

        # Calcule de durée d'activité
        base_duration = distance / speed
        duration_variation = random.uniform(0.9, 1.1)
        duration = int(base_duration * duration_variation)
    else: 
        # Pour les autres sport
        duration = random.randint(1800, 7200)

    return duration

def get_activity_time(current_date: datetime) -> datetime:
    """Cette fonction permet de générer le datetime de début d'activité de sport

    Args:
        current_date (datetime.datetime): information concerne l'horaire du jour actuel

    Returns:
        datetime.datetime: Le Datetime de l'horaire du sport
    """

    # Générer les horaires de l'activé
    if random.random() < 0.3:
        hour = random.randint(6, 9)
    else:
        hour = random.randint(10, 21)

    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return datetime(current_date.year, current_date.month, current_date.day, hour, minute, second)

def sport_activity_generator() -> List[Dict[str, str]]:
        
    """Générer une liste d'activité de sport pour la durée d'un an passé

    Args:

    Return:
        sport_list(List[Dict[str, str]]): Une liste d'activé de sport 
    """
    # Iniialiser  la table pour stocker les informations des activités
    sport_list = list()

    today = datetime.today()

    # Définir les dats limite des activités
    start_day = today - timedelta(days=366)
    end_day = today - timedelta(days=1)

    print(f"Date de début {start_day.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"Date de fin {end_day.strftime('%d/%m/%Y %H:%M:%S')}")

    # Récupérer la liste des salariés dans la base de données
    activity_list = get_employee_ids(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT)
    ids_nb = len(activity_list)

    if ids_nb:
        print(f'{len(activity_list)} salariés trouvés dans la base de données.')
    else:
        print('Aucun salarié trouvé dans la base de données.')

    current_date = start_day

    if ids_nb:
        # Itérer les dates entre start_day et end_day s'il y a des salariés
        while current_date < end_day:
            
            for row in activity_list:
            
                # Cela donne un décimal aléatoire entre 0 et 1
                activity_random_score = random.random()

                # Si c'est un jour de weekend on a plusieur de temps donc plus de chance
                if (start_day.weekday() in (5, 6) and activity_random_score > 0.85) or activity_random_score > 0.95:
                    sport_type = random.choice(SPORT_TYPES)
                else:
                    sport_type = None

                # Calculer la distance en fonction de type de sport
                distance = get_distance(sport_type)

                # Durée en fonction du type de sport et de la distance
                duration = get_duration(sport_type, distance)
                
                # Générer la date et horaire du sport
                start_time = get_activity_time(current_date)

                # Si le type de sport est bien dans la liste des sport, on ajoute les informations de l'activité dans la liste
                if sport_type:
                    sport_list.append((
                        row[0],  
                        row[1],
                        row[2],
                        start_time.strftime('%Y-%m-%d %H:%M:%S'),
                        sport_type, 
                        distance,
                        duration,
                        random.choice(COMMENTS)
                    ))
                
            # Pour la prochaine itération current_date est le jour suivant
            current_date = current_date + timedelta(days=1)

    return sport_list


if  __name__ == "__main__":

    # Compter le nombre historique d'activité des salariés
    activities_nb = get_activitis_nb(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT)

    # Générer une liste d'activité sport
    sport_list = sport_activity_generator()
    print(sport_list[:5])

    # Insérer ces information dans la table d'activé de sport dans bdd
    bulk_insert_sport_activities(list_data=sport_list, host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT)
    
    print(f"{len(sport_list)} activités sportives a été ajouté !")
