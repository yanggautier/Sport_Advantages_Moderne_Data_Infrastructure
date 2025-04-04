----------------- Initialisation de Base de données HR -------------------------
-- Création d'un schéma dédié pour les données du projet
CREATE SCHEMA IF NOT EXISTS sport_advantages;

-- Configuration des autorisations
ALTER SCHEMA sport_advantages OWNER TO sportadvantages;

-- Table des employés
CREATE TABLE sport_advantages.employees(
    id_employee INT PRIMARY KEY,
    first_name VARCHAR(20) NOT NULL,
    last_name VARCHAR(20) NOT NULL,
    birthday DATE,
    business_unity VARCHAR(20),
    hire_date DATE,
    gross_salary INT,
    constract_type VARCHAR(5),
    address VARCHAR(255),
    transport_mode VARCHAR(100),
    paid_leaved_days INT

);

-- Table pour stocker les validations de distance
CREATE TABLE sport_advantages.commute_validations(
    id_validate SERIAL PRIMARY KEY,
    id_employee INT NOT NULL,
    calculed_distance NUMERIC(10, 2),
    calculed_duration NUMERIC(10, 2),
    is_valid BOOLEAN,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_employee) REFERENCES sport_advantages.employees(id_employee)
);

-- Table pour stocker les validations de distance
CREATE TABLE sport_advantages.sport_activities(
    id SERIAL PRIMARY KEY,
    id_employee INT NOT NULL,
    start_datetime TIMESTAMP,
    sport_type VARCHAR(20),
    distance SMALLINT, 
    activity_duration SMALLINT,
    Commentaire VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (id_employee) REFERENCES sport_advantages.employees(id_employee)
);

-- Table pour les avantages calculés
CREATE TABLE IF NOT EXISTS sport_advantages.sport_advantages (
    id SERIAL PRIMARY KEY,
    id_employee INT NOT NULL,
    calculation_date DATE NOT NULL,
    sport_bonus_percentage NUMERIC(5, 2) DEFAULT 0,
    sport_bonus_amount NUMERIC(10, 2) DEFAULT 0,
    wellness_days_earned INTEGER DEFAULT 0,
    total_activities INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_employee) REFERENCES sport_advantages.employees(id_employee),
    UNIQUE (id_employee, calculation_date)
);

---------------------------- Configuration pour Debezium ----------------------------------
-- Activer l'extension pour la réplication logique (nécessaire pour Debezium)
ALTER SYSTEM SET wal_level='logical'

-- Création de la table de publication pour Debezium
CREATE PUBLICATION sport_advantages_publication FOR TABLE sport_advantages.sport_activities;


---------------------------- Configuration de sécurité ----------------------------------
-- Autorisations
GRANT ALL PRIVILEGES ON SCHEMA sport_advantages TO sportadvantagehr;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA sport_advantages TO sportadvantagehr;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA sport_advantages TO sportadvantagehr;

-- Créer un rôle en lecture seule pour les rapports
CREATE ROLE reporting;
GRANT CONNECT ON DATABASE sportadvantages TO reporting;
GRANT USAGE ON SCHEMA sport_advantages TO reporting;
GRANT SELECT ON ALL TABLES IN SCHEMA sport_advantages TO reporting;

-- Créer un utilisateur pour reporting PowerBI
CREATE USER sportadvantagebiuser WITH PASSWORD 'bipassword';
GRANT reporting TO sportadvantagebiuser;

-- Création de view pour masquer les salaires pour utilisateurs bi
CREATE VIEW sport_advantages.employees_masked AS
SELECT 
    id_employee, first_name, last_name, hire_date, business_unity, constract_type, paid_leaved_days,
    CASE WHEN current_user = 'sportadvantagebiuser' 
        THEN NULL 
    ELSE gross_salary 
    END AS gross_salary
FROM sport_advantages.employees;

-- Donner accès uniquement à la vue pour powerbi_user
GRANT SELECT ON sport_advantages.employees_masked TO sportadvantagebiuser;