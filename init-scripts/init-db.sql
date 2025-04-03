CREATE IF NOT EXISTS DATABASE sport;

CREATE IF NOT EXISTS SCHEMA sport_advantage;

CREATE TABLE sport_advantage.employee (
    id_empolyee INT NOT NULL,
    lastname VARCHAR(20) NOT NULL,
    firstname VARCHAR(20) NOT NULL,
    birthday DATE,
    business_unity VARCHAR(20),
    hiredate DATE,
    gross_salary SMALLINT,
    constract_type VARCHAR(5),
    CP_days SMALLINT,
    CONSTRAINT employee_pk PRIMARY KEY (id_empolyee),
);


CREATE TABLE sport_advantage.transport_validation (
    id_validate SERIAL INT,
    id_empolyee INT,
    address VARCHAR(200),
    transport_mode VARCHAR(100),
    calculed_distance DECIMAL,
    calculed_duration DECIMAL,
    id_validate BOOLEAN,
    raison VARCHAR(200),
    CONSTRAINT validation_pk PRIMARY KEY (id_validate),
    CONSTRAINT fk_alidation_employee FOREIGN KEY (id_empolyee)
            REFERENCES sport_advantage.employee(id_empolyee) ON DELETE CASCADE
);

CREATE TABLE sport_advantage.advantage(
    
)


COPY inflation_data
FROM '/docker-entrypoint-initdb.d/inflation.csv'
DELIMITER ','
CSV HEADER;



