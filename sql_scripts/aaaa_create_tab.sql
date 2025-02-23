-- Удаление существующих таблиц
DROP TABLE IF EXISTS st.aaaa_dwh_dim_accounts_hist;
DROP TABLE IF EXISTS st.aaaa_dwh_dim_cards_hist;
DROP TABLE IF EXISTS st.aaaa_dwh_dim_clients_hist;
DROP TABLE IF EXISTS st.aaaa_dwh_dim_terminals_hist;
DROP TABLE IF EXISTS st.aaaa_dwh_fact_passport_blacklist;
DROP TABLE IF EXISTS st.aaaa_dwh_fact_transactions;
DROP TABLE IF EXISTS st.aaaa_meta_load_info;
DROP TABLE IF EXISTS st.aaaa_rep_fraud;
DROP TABLE IF EXISTS st.aaaa_stg_passport_blacklist;
DROP TABLE IF EXISTS st.aaaa_stg_terminals;
DROP TABLE IF EXISTS st.aaaa_stg_transactions;
DROP TABLE IF EXISTS st.aaaa_event_type;

-- Select по созданным таблицам

select * from st.aaaa_dwh_dim_accounts_hist
select * from st.aaaa_dwh_dim_cards_hist
select * from st.aaaa_dwh_dim_clients_hist
select * from st.aaaa_dwh_dim_terminals_hist
select * from st.aaaa_dwh_fact_passport_blacklist
select * from st.aaaa_dwh_fact_transactions
select * from st.aaaa_meta_load_info
select * from st.aaaa_rep_fraud
select * from st.aaaa_stg_passport_blacklist
select * from st.aaaa_stg_terminals
select * from st.aaaa_stg_transactions
select * from st.aaaa_event_type

-- Очистка stg таблиц
truncate st.aaaa_stg_passport_blacklist;
truncate st.aaaa_stg_terminals;
truncate st.aaaa_stg_transactions;

-- Проверка и подсчёт на дубликаты
select 
	count(distinct event_dt),
	event_id
from st.aaaa_rep_fraud
group by event_id


-- Создание таблиц
CREATE TABLE IF NOT EXISTS st.aaaa_stg_transactions (
    trans_id VARCHAR(50),
    trans_date TIMESTAMP,
    amt DECIMAL(18, 2),
    card_num VARCHAR(20),
    oper_type VARCHAR(20),
    oper_result VARCHAR(20),
    terminal VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS st.aaaa_stg_terminals (
    terminal_id VARCHAR(20),
    terminal_type VARCHAR(20),
    terminal_city VARCHAR(100),
    terminal_address VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS st.aaaa_stg_passport_blacklist (
    date DATE,
    passport_num VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS st.aaaa_dwh_fact_transactions (
    trans_id VARCHAR(50) PRIMARY KEY,
    trans_date TIMESTAMP,
    amt DECIMAL(18, 2),
    card_num VARCHAR(20),
    oper_type VARCHAR(20),
    oper_result VARCHAR(20),
    terminal VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS st.aaaa_dwh_fact_passport_blacklist (
    date DATE,
    passport_num VARCHAR(20) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS st.aaaa_dwh_dim_terminals_hist (
    terminal_id VARCHAR(20),
    terminal_type VARCHAR(20),
    terminal_city VARCHAR(100),
    terminal_address VARCHAR(255),
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    deleted_flg CHAR(1)
);

CREATE TABLE IF NOT EXISTS st.aaaa_dwh_dim_accounts_hist (
    account_num VARCHAR(20),
    valid_to DATE,
    client_id VARCHAR(10),
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    deleted_flg CHAR(1)
);

CREATE TABLE IF NOT EXISTS st.aaaa_dwh_dim_cards_hist (
    card_num VARCHAR(20),
    account_num VARCHAR(20),
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    deleted_flg CHAR(1)
);

CREATE TABLE IF NOT EXISTS st.aaaa_dwh_dim_clients_hist (
    client_id VARCHAR(10),
    last_name VARCHAR(20),
    first_name VARCHAR(20),
    patronymic VARCHAR(20),
    date_of_birth DATE,
    passport_num VARCHAR(20),
    passport_valid_to DATE,
    phone VARCHAR(20),
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    deleted_flg CHAR(1)
);

create table if not exists st.aaaa_event_type (
	event_id int,
	event Text
);


CREATE TABLE IF NOT EXISTS st.aaaa_rep_fraud (
    event_dt TIMESTAMP,
    passport VARCHAR(20),
    fio VARCHAR(255),
    phone VARCHAR(20),
    event_id int,
    report_dt TIMESTAMP
);

CREATE TABLE IF NOT EXISTS st.aaaa_meta_load_info (
    load_date DATE,
    file_name VARCHAR(255),
    load_status VARCHAR(50),
    error_message TEXT
);

INSERT INTO st.aaaa_event_type (event_id, event) VALUES 
(1, 'Просроченный или заблокированный паспорт'),
(2, 'Недействующий договор'),
(3, 'Операции в разных городах в течение одного часа'),
(4, 'Попытка подбора суммы');
			
			