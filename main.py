from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
import os

# Определите переменные для компонентов строки подключения
username = 'your_username'
password = 'your_password'
host = 'your_host'
port = 'your_port'
database = 'your_database'

# Создайте строку подключения, используя переменные
connection_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'

def load_transactions(file_path):
    if file_path.endswith('.txt'):
        df = pd.read_table(file_path, sep=';', parse_dates=['transaction_date'])
    elif file_path.endswith('.csv'):
        df = pd.read_csv(file_path, sep=',', parse_dates=['transaction_date'])
    else:
        raise ValueError(f"Неподдерживаемый формат файла: {file_path}")
    
    df['amount'] = df['amount'].str.replace(',', '.').astype(float)
    
    df.rename(columns={
        'transaction_id': 'trans_id',
        'transaction_date': 'trans_date',
        'amount': 'amt'
    }, inplace=True)
    
    df['trans_id'] = df['trans_id'].astype(str)
    
    return df

def load_terminals(file_path):
    df = pd.read_excel(file_path, sheet_name='terminals')
    return df

def load_passport_blacklist(file_path):
    df = pd.read_excel(file_path, sheet_name='blacklist', parse_dates=['date'])
    
    df.rename(columns={
        'date': 'date',
        'passport': 'passport_num'
    }, inplace=True)
    
    return df

def move_to_archive(file_path):
    try:
        archive_path = os.path.join('archive', os.path.basename(file_path) + '.backup')
        if not os.path.exists('archive'):
            os.makedirs('archive')
        os.rename(file_path, archive_path)
    except Exception as e:
        print(f"Ошибка при перемещении файла в архив: {e}")

def log_load_info(file_name, load_status, error_message=None):
    load_date = datetime.now().date()
    with engine.connect() as connection:
        try:
            connection.execute(text(
                "INSERT INTO st.aaaa_meta_load_info (load_date, file_name, load_status, error_message) VALUES (:load_date, :file_name, :load_status, :error_message)"),
                {"load_date": load_date, "file_name": file_name, "load_status": load_status, "error_message": error_message}
            )
            connection.commit()  
        except Exception as e:
            print(f"Ошибка при логировании: {e}")

def process_file(file_type, file_path):
    if not os.path.exists(file_path):
        log_load_info(file_path, 'Failure', f"Файл {file_path} не найден")
        return
    try:
        if file_type == 'transactions':
            df = load_transactions(file_path)
            df.to_sql('aaaa_stg_transactions', con=engine, schema='st', if_exists='replace', index=False)
            print(f"Данные из файла {file_path} успешно загружены в таблицу aaaa_stg_transactions")
        elif file_type == 'terminals':
            df = load_terminals(file_path)
            df.to_sql('aaaa_stg_terminals', con=engine, schema='st', if_exists='replace', index=False)
            print(f"Данные из файла {file_path} успешно загружены в таблицу aaaa_stg_terminals")
        elif file_type == 'passport_blacklist':
            df = load_passport_blacklist(file_path)
            df.to_sql('aaaa_stg_passport_blacklist', con=engine, schema='st', if_exists='replace', index=False)
            print(f"Данные из файла {file_path} успешно загружены в таблицу aaaa_stg_passport_blacklist")
        
        move_to_archive(file_path)
        log_load_info(file_path, 'Success')
    except Exception as e:
        log_load_info(file_path, 'Failure', str(e))
        print(f"Ошибка при обработке файла {file_path}: {e}")

def load_dwh_data():
    with engine.connect() as connection:
        try:
            transaction = connection.begin()

            connection.execute(text("""
                INSERT INTO st.aaaa_dwh_fact_transactions (trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal)
                SELECT trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal
                FROM st.aaaa_stg_transactions
                WHERE trans_id NOT IN (SELECT trans_id FROM st.aaaa_dwh_fact_transactions);
            """))

            connection.execute(text("""
                INSERT INTO st.aaaa_dwh_fact_passport_blacklist (date, passport_num)
                SELECT date, passport_num
                FROM st.aaaa_stg_passport_blacklist
                WHERE passport_num NOT IN (SELECT passport_num FROM st.aaaa_dwh_fact_passport_blacklist);
            """))

            connection.execute(text("""
                WITH updated AS (
                    UPDATE st.aaaa_dwh_dim_terminals_hist
                    SET effective_to = NOW() - INTERVAL '1 second', deleted_flg = 'Y'
                    WHERE terminal_id IN (SELECT terminal_id FROM st.aaaa_stg_terminals)
                      AND effective_to = '2999-12-31 23:59:59'
                    RETURNING terminal_id
                )
                INSERT INTO st.aaaa_dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
                SELECT terminal_id, terminal_type, terminal_city, terminal_address, NOW(), '2999-12-31 23:59:59', 'N'
                FROM st.aaaa_stg_terminals
                WHERE terminal_id NOT IN (SELECT terminal_id FROM updated);
            """))

            connection.execute(text("""
                WITH updated AS (
                    UPDATE st.aaaa_dwh_dim_cards_hist
                    SET effective_to = NOW() - INTERVAL '1 second', deleted_flg = 'Y'
                    WHERE card_num IN (SELECT card_num FROM st.aaaa_stg_transactions)
                      AND effective_to = '2999-12-31 23:59:59'
                    RETURNING card_num
                )
                INSERT INTO st.aaaa_dwh_dim_cards_hist (card_num, account_num, effective_from, effective_to, deleted_flg)
                SELECT DISTINCT ON (cn.card_num) cn.card_num, account, NOW(), '2999-12-31 23:59:59', 'N'
                FROM bank.cards cn
                INNER JOIN st.aaaa_stg_transactions tr ON cn.card_num = tr.card_num
                WHERE cn.card_num NOT IN (SELECT card_num FROM updated);
            """))

            connection.execute(text("""
                WITH updated AS (
                    UPDATE st.aaaa_dwh_dim_accounts_hist
                    SET effective_to = NOW() - INTERVAL '1 second', deleted_flg = 'Y'
                    WHERE account_num IN (
                        SELECT ba.account
                        FROM st.aaaa_stg_transactions tr
                        INNER JOIN bank.cards bc ON bc.card_num = tr.card_num
                        INNER JOIN bank.accounts ba ON ba.account = bc.account
                    )
                    AND effective_to = '2999-12-31 23:59:59'
                    RETURNING account_num
                )
                INSERT INTO st.aaaa_dwh_dim_accounts_hist (account_num, valid_to, client_id, effective_from, effective_to, deleted_flg)
                SELECT DISTINCT ON (ba.account) ba.account, ba.valid_to, ba.client, NOW(), '2999-12-31 23:59:59', 'N'
                FROM st.aaaa_stg_transactions tr
                INNER JOIN bank.cards bc ON bc.card_num = tr.card_num
                INNER JOIN bank.accounts ba ON bc.account = ba.account
                WHERE ba.account NOT IN (SELECT account_num FROM updated);
            """))

            connection.execute(text("""
                WITH updated AS (
                    UPDATE st.aaaa_dwh_dim_clients_hist
                    SET effective_to = NOW() - INTERVAL '1 second', deleted_flg = 'Y'
                    WHERE client_id IN (
                        SELECT bcl.client_id
                        FROM st.aaaa_stg_transactions tr
                        INNER JOIN bank.cards bc ON bc.card_num = tr.card_num
                        INNER JOIN bank.accounts ba ON ba.account = bc.account
                        INNER JOIN bank.clients bcl ON bcl.client_id = ba.client
                    )
                    AND effective_to = '2999-12-31 23:59:59'
                    RETURNING client_id
                )
                INSERT INTO st.aaaa_dwh_dim_clients_hist (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
                SELECT DISTINCT ON (bcl.client_id) bcl.client_id, bcl.last_name, bcl.first_name, bcl.patronymic, bcl.date_of_birth, bcl.passport_num, bcl.passport_valid_to, bcl.phone, NOW(), '2999-12-31 23:59:59', 'N'
                FROM st.aaaa_stg_transactions tr
                INNER JOIN bank.cards bc ON bc.card_num = tr.card_num
                INNER JOIN bank.accounts ba ON bc.account = ba.account
                INNER JOIN bank.clients bcl ON bcl.client_id = ba.client
                WHERE bcl.client_id NOT IN (SELECT client_id FROM updated);
            """))

            transaction.commit()
            print("Данные успешно загружены в DWH")
        except Exception as e:
            transaction.rollback()
            print(f"Ошибка при загрузке данных в DWH: {e}")


def clear_stg_tabs():
    with engine.connect() as connection:
        try:
            transaction = connection.begin()
            connection.execute(text("""
            truncate st.aaaa_stg_passport_blacklist;
            truncate st.aaaa_stg_terminals;
            truncate st.aaaa_stg_transactions;
                                    
        """))
            
            transaction.commit()
            print("STG таблицы очищены")
        except Exception as e:
            transaction.rollback()
            print(f"Ошибка при очистке STG таблиц: {e}")

def drop_temp():
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(text("DROP TABLE IF EXISTS temp_fraud_report"))
            print("Временная таблица удалена")
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            print(f"Ошибка: {e}")


def build_fraud_report():
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            print("Начинаю построение отчета о мошенничестве")
            connection.execute(text("""
            CREATE TEMP TABLE IF NOT EXISTS temp_fraud_report (
                event_dt TIMESTAMP,
                passport TEXT,
                fio TEXT,
                phone TEXT,
                event_id INT,
                report_dt TIMESTAMP
            )
            """))
                # Подбор суммы
            connection.execute(text("""
            INSERT INTO temp_fraud_report
            WITH transaction_sequence AS (
                SELECT DISTINCT
                    dft.trans_date AS event_dt,
                    dft.oper_result AS oper_result,
                    dft.amt AS amt,
                    t3.passport AS passport,
                    t3.fio AS fio,
                    t3.phone AS phone,
                    LAG(dft.amt) OVER (PARTITION BY t3.passport ORDER BY dft.trans_date) AS prev_amt,
                    LAG(dft.oper_result) OVER (PARTITION BY t3.passport ORDER BY dft.trans_date) AS prev_oper_result,
                    ROW_NUMBER() OVER (PARTITION BY t3.passport ORDER BY dft.trans_date) AS transaction_num
                FROM
                    st.aaaa_dwh_fact_transactions AS dft
                INNER JOIN (
                    SELECT DISTINCT
                        car.card_num AS card_num,
                        t2.passport_num AS passport,
                        t2.fio AS fio,
                        t2.phone AS phone
                    FROM
                        st.aaaa_dwh_dim_cards_hist AS car
                    INNER JOIN (
                        SELECT DISTINCT
                            ddah.account_num,
                            stc.fio AS fio,
                            stc.ci AS client_id,
                            stc.pn AS passport_num,
                            stc.phone AS phone
                        FROM
                            st.aaaa_dwh_dim_accounts_hist AS ddah
                        INNER JOIN (
                            SELECT DISTINCT
                                client_id AS ci,
                                last_name || ' ' || first_name || ' ' || patronymic AS fio,
                                passport_num AS pn,
                                phone AS phone
                            FROM
                                st.aaaa_dwh_dim_clients_hist
                        ) AS stc ON ddah.client_id = stc.ci
                    ) AS t2 ON t2.account_num = car.account_num
                ) AS t3 ON t3.card_num = dft.card_num
            ),
            fraud_candidates AS (
                SELECT DISTINCT
                    event_dt,
                    passport,
                    fio,
                    phone,
                    amt,
                    oper_result,
                    transaction_num,
                    COUNT(*) OVER (PARTITION BY passport ORDER BY event_dt RANGE BETWEEN INTERVAL '20 minutes' PRECEDING AND CURRENT ROW) AS transaction_count,
                    LAG(oper_result, 1) OVER (PARTITION BY passport ORDER BY event_dt) AS prev_oper_result_1,
                    LAG(oper_result, 2) OVER (PARTITION BY passport ORDER BY event_dt) AS prev_oper_result_2,
                    LAG(amt, 1) OVER (PARTITION BY passport ORDER BY event_dt) AS prev_amt_1,
                    LAG(amt, 2) OVER (PARTITION BY passport ORDER BY event_dt) AS prev_amt_2
                FROM
                    transaction_sequence
            ),
            fraud_detection AS (
                SELECT DISTINCT
                    event_dt,
                    passport,
                    fio,
                    phone,
                    amt AS last_successful_amt,
                    'Попытка подбора суммы' AS event_type,
                    transaction_count,
                    current_timestamp AS report_dt
                FROM
                    fraud_candidates
                WHERE
                    oper_result = 'SUCCESS' -- Последняя операция успешна
                    AND prev_oper_result_1 = 'REJECT' -- Предыдущая операция отклонена
                    AND prev_oper_result_2 = 'REJECT' -- Предыдущая операция отклонена
                    AND amt < prev_amt_1 -- Текущая сумма меньше предыдущей
                    AND prev_amt_1 < prev_amt_2 -- Предыдущая сумма меньше предыдущей
                    AND transaction_count >= 3 -- Более 3 операций в течение 20 минут
            )
            SELECT DISTINCT
                event_dt,
                passport,
                fio,
                phone,
                4 as event_id,
                report_dt
            FROM
                fraud_detection;
            """))

            #Паспорт в черном списке
            connection.execute(text("""
            INSERT INTO temp_fraud_report
            SELECT DISTINCT
                t.trans_date,
                c.passport_num AS passport,
                c.last_name || ' ' || c.first_name || ' ' || c.patronymic AS fio,
                c.phone AS phone,
                1 AS event_id,
                CURRENT_TIMESTAMP AS report_dt
            FROM
                st.aaaa_dwh_fact_transactions t
            JOIN
                st.aaaa_dwh_dim_cards_hist ch
            ON
                t.card_num = ch.card_num
            JOIN
                st.aaaa_dwh_dim_accounts_hist a
            ON
                ch.account_num = a.account_num
            JOIN
                st.aaaa_dwh_dim_clients_hist c
            ON
                a.client_id = c.client_id
            JOIN
                st.aaaa_dwh_fact_passport_blacklist p
            ON
                c.passport_num = p.passport_num
            WHERE
                t.trans_date > p.date  -- Транзакция совершена после попадания паспорта в черный список
                OR t.trans_date > c.passport_valid_to  -- Транзакция совершена после истечения срока действия паспорта
            """))

            #Истекший срок действия договора
            connection.execute(text("""
            INSERT INTO temp_fraud_report
            SELECT DISTINCT
                t.trans_date,
                t2.passport_num AS passport,
                t2.fio as fio,
                t2.phone as phone,
                2 as event_id,
                current_timestamp as report_dt
            FROM
                st.aaaa_dwh_fact_transactions t
            JOIN
                st.aaaa_dwh_dim_cards_hist c ON t.card_num = c.card_num
            JOIN
                st.aaaa_dwh_dim_accounts_hist a ON c.account_num = a.account_num
            JOIN
                (SELECT DISTINCT
                    ddah.account_num,
                    stc.fio AS fio,
                    stc.ci AS client_id,
                    stc.pn AS passport_num,
                    stc.phone AS phone
                FROM
                    st.aaaa_dwh_dim_accounts_hist AS ddah
                INNER JOIN
                    (SELECT DISTINCT
                        client_id AS ci,
                        last_name || ' ' || first_name || ' ' || patronymic AS fio,
                        passport_num AS pn,
                        phone AS phone
                    FROM
                        st.aaaa_dwh_dim_clients_hist) AS stc
                ON
                    ddah.client_id = stc.ci
                ) AS t2
            ON
                a.account_num = t2.account_num
            WHERE
                t.trans_date > a.valid_to
            """))

            #Транзакции в разных городах в течение одного часа
            connection.execute(text("""
            INSERT INTO temp_fraud_report
            WITH transaction_cities AS (
                SELECT DISTINCT
                    t.trans_id,
                    t.trans_date,
                    t.card_num,
                    t.terminal,
                    tr.terminal_city
                FROM
                    st.aaaa_dwh_fact_transactions t
                JOIN
                    st.aaaa_dwh_dim_terminals_hist tr
                ON
                    t.terminal = tr.terminal_id
            ),
            suspicious_transactions AS (
                SELECT DISTINCT
                    t1.trans_id AS trans_id_1,
                    t2.trans_id AS trans_id_2,
                    t1.trans_date AS trans_date_1,
                    t2.trans_date AS trans_date_2,
                    t1.card_num,
                    t1.terminal_city AS city_1,
                    t2.terminal_city AS city_2
                FROM
                    transaction_cities t1
                JOIN
                    transaction_cities t2
                ON
                    t1.card_num = t2.card_num
                WHERE
                    t1.trans_id <> t2.trans_id  -- Исключаем сравнение транзакции с самой собой
                    AND t1.trans_date <= t2.trans_date  -- Убеждаемся, что первая транзакция была раньше или в то же время
                    AND t2.trans_date <= t1.trans_date + INTERVAL '1 hour'  -- Операции в течение одного часа
                    AND t1.terminal_city <> t2.terminal_city  -- Операции в разных городах
            ),
            client_info AS (
                SELECT DISTINCT
                    c.client_id,
                    c.passport_num,
                    c.last_name || ' ' || c.first_name || ' ' || c.patronymic AS fio,
                    c.phone
                FROM
                    st.aaaa_dwh_dim_clients_hist c
            )
            SELECT DISTINCT
                st.trans_date_2 AS event_dt,
                c.passport_num AS passport,
                c.fio,
                c.phone,
                3 AS event_id,  -- Указываем тип события "Операции в разных городах в течение одного часа"
                CURRENT_TIMESTAMP AS report_dt
            FROM
                suspicious_transactions st
            JOIN
                st.aaaa_dwh_dim_cards_hist card
            ON
                st.card_num = card.card_num
            JOIN
                st.aaaa_dwh_dim_accounts_hist acc
            ON
                card.account_num = acc.account_num
            JOIN
                client_info c
            ON
                acc.client_id = c.client_id;
            """))

            # Вставка только новых записей в rep_fraud
            connection.execute(text("""
            INSERT INTO st.aaaa_rep_fraud (event_dt, passport, fio, phone, event_id, report_dt)
            SELECT DISTINCT event_dt, passport, fio, phone, event_id, report_dt
            FROM temp_fraud_report
            WHERE (event_dt, passport, fio, phone, event_id) NOT IN (
                SELECT DISTINCT event_dt, passport, fio, phone, event_id
                FROM st.aaaa_rep_fraud
            );
            """))

            # Логирование для отладки
            #result = connection.execute(text("SELECT COUNT(*) FROM st.aaaa_rep_fraud"))
            #count = result.fetchone()[0]
            #print(f"Добавлено {count} записей в отчет о мошенничестве.")

            transaction.commit()

        except Exception as e:
            transaction.rollback()
            print(f"Ошибка: {e}")



def main():
    # Списки файлов для каждого типа данных
    transactions_files = ['transactions_01032021.txt', 'transactions_02032021.txt', 'transactions_03032021.txt']  
    terminals_files = ['terminals_01032021.xlsx', 'terminals_02032021.xlsx', 'terminals_03032021.xlsx']
    passport_blacklist_files = ['passport_blacklist_01032021.xlsx', 'passport_blacklist_02032021.xlsx', 'passport_blacklist_03032021.xlsx']

    for transactions_file, terminals_file, passport_blacklist_file in zip(transactions_files, terminals_files, passport_blacklist_files):
        clear_stg_tabs()

        process_file('transactions', transactions_file)
        process_file('terminals', terminals_file)
        process_file('passport_blacklist', passport_blacklist_file)
        
        load_dwh_data()
        build_fraud_report()

    print("Дело сделано!")
    drop_temp()

if __name__ == "__main__":
    main()