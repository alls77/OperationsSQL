-- Створення таблиці
CREATE TABLE IF NOT EXISTS operations_t1 (
    operation_date TIMESTAMPTZ NOT NULL,
    operation_id BIGINT NOT NULL,
    amount NUMERIC(15, 2) NOT NULL CHECK (amount >= 0),
    status SMALLINT NOT NULL DEFAULT 0 CHECK (status IN (0, 1)),
    operation_guid UUID NOT NULL DEFAULT gen_random_uuid(),
    message JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (operation_date, operation_id)
) PARTITION BY RANGE (operation_date);

-- Sequence для генерації operation_id
CREATE SEQUENCE IF NOT EXISTS operations_id_seq START WITH 1;

-- Створення індексів
CREATE INDEX IF NOT EXISTS idx_operations_status 
    ON operations_t1 (status) 
    WHERE status = 0;

CREATE INDEX IF NOT EXISTS idx_operations_guid 
    ON operations_t1 USING HASH (operation_guid);

CREATE INDEX IF NOT EXISTS idx_operations_client_type 
    ON operations_t1 USING BTREE ((message->>'client_id'), (message->>'operation_type'));

CREATE INDEX IF NOT EXISTS idx_operations_id
    ON operations_t1 (operation_id);

-- Забезпечення унікальності operation_guid
CREATE TABLE IF NOT EXISTS operation_guids (
    operation_guid UUID PRIMARY KEY,
    operation_date TIMESTAMPTZ NOT NULL,
    operation_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_guids_date_id 
    ON operation_guids (operation_date, operation_id);

-- Trigger для синхронізації operation_guids з operations_t1
CREATE OR REPLACE FUNCTION check_and_register_guid()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO operation_guids (operation_guid, operation_date, operation_id)
    VALUES (NEW.operation_guid, NEW.operation_date, NEW.operation_id);

    RETURN NEW;
    
EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'Duplicate operation_guid: %', NEW.operation_guid
            USING ERRCODE = '23505';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_guid_uniqueness
    BEFORE INSERT ON operations_t1
    FOR EACH ROW
    EXECUTE FUNCTION check_and_register_guid();


-- Trigger для видалення з operation_guids
CREATE OR REPLACE FUNCTION cleanup_guid_on_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM operation_guids 
    WHERE operation_guid = OLD.operation_guid;
    
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_cleanup_guid
    AFTER DELETE ON operations_t1
    FOR EACH ROW
    EXECUTE FUNCTION cleanup_guid_on_delete();

-- Функція для автоматичного створення партицій
CREATE OR REPLACE FUNCTION create_partition_if_not_exists(
    p_date DATE
) RETURNS VOID AS $$
DECLARE
    v_partition_name TEXT;
    v_start_date DATE;
    v_end_date DATE;
BEGIN
    v_partition_name := 'operations_t1_' || TO_CHAR(p_date, 'YYYY_MM');
    v_start_date := DATE_TRUNC('month', p_date);
    v_end_date := v_start_date + INTERVAL '1 month';
    
    IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = v_partition_name
    ) THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF operations_t1 
             FOR VALUES FROM (%L) TO (%L)',
            v_partition_name,
            v_start_date,
            v_end_date
        );
        
        RAISE NOTICE 'Created partition: %', v_partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- Створення партицій
DO $$
DECLARE
    v_date DATE := CURRENT_DATE - INTERVAL '3 months';
    v_end_date DATE := CURRENT_DATE + INTERVAL '1 month';
BEGIN
    WHILE v_date <= v_end_date LOOP
        PERFORM create_partition_if_not_exists(v_date);
        v_date := v_date + INTERVAL '1 month';
    END LOOP;
END $$;

-- Генерація тестових даних
CREATE OR REPLACE FUNCTION generate_test_data(
    p_num_records INTEGER DEFAULT 100000,
    p_months_back INTEGER DEFAULT 3
) RETURNS TABLE (
    inserted_count INTEGER,
    date_from TIMESTAMPTZ,
    date_to TIMESTAMPTZ
) AS $$
DECLARE
    v_start_date TIMESTAMPTZ;
    v_end_date TIMESTAMPTZ;
    v_inserted INTEGER := 0;
BEGIN
    v_start_date := CURRENT_TIMESTAMP - (p_months_back || ' months')::INTERVAL;
    v_end_date := CURRENT_TIMESTAMP;
    
    RAISE NOTICE 'Starting data generation: % records from % to %', 
        p_num_records, v_start_date, v_end_date;
    
    INSERT INTO operations_t1 (
        operation_date,
        operation_id,
        amount,
        status,
        operation_guid,
        message
    )
    SELECT 
        v_start_date + (random() * (v_end_date - v_start_date)) AS operation_date,
        nextval('operations_id_seq') AS operation_id,
        ROUND((random() * 10000)::NUMERIC, 2) AS amount,
        CASE WHEN random() < 0.7 THEN 0 ELSE 1 END AS status,
        gen_random_uuid() AS operation_guid,
        jsonb_build_object(
            'account_number', 'ACC' || LPAD((random() * 999999)::INTEGER::TEXT, 6, '0'),
            'client_id', (random() * 1000)::INTEGER + 1,
            'operation_type', CASE WHEN random() < 0.6 THEN 'online' ELSE 'offline' END
        ) AS message
    FROM generate_series(1, p_num_records);
    
    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    
    RAISE NOTICE 'Data generation completed: % records inserted', v_inserted;
    
    RETURN QUERY SELECT v_inserted, v_start_date, v_end_date;
END;
$$ LANGUAGE plpgsql;

-- Викликати генерацію даних
SELECT * FROM generate_test_data(100000, 3);

-- Функція додавання запису зі статусом = 0
CREATE OR REPLACE FUNCTION insert_new_operation()
RETURNS VOID AS $$
DECLARE
    v_operation_date TIMESTAMPTZ := CURRENT_TIMESTAMP;
BEGIN
    PERFORM create_partition_if_not_exists(v_operation_date::DATE);
    
    INSERT INTO operations_t1 (
        operation_date,
        operation_id,
        amount,
        status,
        operation_guid,
        message
    ) VALUES (
        v_operation_date,
        nextval('operations_id_seq'),
        ROUND((random() * 5000)::NUMERIC, 2),
        0,
        gen_random_uuid(),
        jsonb_build_object(
            'account_number', 'ACC' || LPAD((random() * 999999)::INTEGER::TEXT, 6, '0'),
            'client_id', (random() * 1000)::INTEGER + 1,
            'operation_type', CASE WHEN random() < 0.6 THEN 'online' ELSE 'offline' END
        )
    );
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error inserting operation: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- Функція оновлення статусу (парні/непарні айді)
CREATE OR REPLACE FUNCTION update_operation_status()
RETURNS TABLE (updated_count INTEGER) AS $$
DECLARE
    v_current_second INTEGER;
    v_is_even BOOLEAN;
    v_updated INTEGER := 0;
BEGIN
    -- Отримуємо поточну секунду
    v_current_second := EXTRACT(SECOND FROM CURRENT_TIMESTAMP)::INTEGER;
    v_is_even := (v_current_second % 2) = 0;
    
    UPDATE operations_t1
    SET 
        status = 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE 
        status = 0
        AND (operation_id % 2) = CASE WHEN v_is_even THEN 0 ELSE 1 END
        AND operation_date >= CURRENT_DATE - INTERVAL '1 day';
    
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    
    RAISE NOTICE 'Updated % records (second: %, even: %)', 
        v_updated, v_current_second, v_is_even;
    
    RETURN QUERY SELECT v_updated;
END;
$$ LANGUAGE plpgsql;

-- Створення materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_client_operation_sums AS
SELECT 
    (message->>'client_id')::INTEGER AS client_id,
    message->>'operation_type' AS operation_type,
    COUNT(*) AS operation_count,
    SUM(amount) AS total_amount
FROM operations_t1
WHERE status = 1
GROUP BY 
    (message->>'client_id')::INTEGER,
    message->>'operation_type'
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_client_operation_pk 
    ON mv_client_operation_sums (client_id, operation_type);

CREATE INDEX IF NOT EXISTS idx_mv_total_amount 
    ON mv_client_operation_sums (total_amount DESC);

-- Функція для інкрементального оновлення materialized view
CREATE OR REPLACE FUNCTION refresh_mv_client_sums()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_client_operation_sums;
    
    RAISE NOTICE 'Materialized view refreshed at %', CURRENT_TIMESTAMP;
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error refreshing materialized view: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- Тригер для автоматичного оновлення MV при зміні статусу
CREATE OR REPLACE FUNCTION trigger_refresh_mv_on_status_change()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 1 AND OLD.status = 0 THEN
        PERFORM refresh_mv_client_sums();
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- Налаштування pg_timetable
-- Регламентне завдання #1: додавання запису кожні 5 секунд
DO $$
DECLARE
    v_chain_id BIGINT;
BEGIN
    -- Видаляємо попереднє завдання якщо існує
    DELETE FROM timetable.chain WHERE chain_name = 'insert_operation_every_5sec';
    
    -- Створюємо новий chain
    INSERT INTO timetable.chain (
        chain_name,
        run_at_minute,
        run_at_hour,
        run_at_day,
        run_at_month,
        run_at_day_of_week,
        max_instances,
        live,
        self_destruct,
        exclusive_execution,
        excluded_execution_configs
    ) VALUES (
        'insert_operation_every_5sec',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        1,
        TRUE,
        FALSE,
        TRUE,
        NULL
    ) RETURNING chain_id INTO v_chain_id;
    
    -- Додаємо SQL команду до chain
    INSERT INTO timetable.task (
        chain_id,
        task_order,
        kind,
        command,
        run_as,
        ignore_error
    ) VALUES (
        v_chain_id,
        1,
        'SQL'::timetable.command_kind,
        'SELECT insert_new_operation()',
        NULL,
        FALSE
    );
    
    RAISE NOTICE 'Created task: insert_operation_every_5sec (chain_id: %)', v_chain_id;
END $$;

-- Регламентне завдання #2: оновлення статусу кожні 3 секунди
DO $$
DECLARE
    v_chain_id BIGINT;
BEGIN
    DELETE FROM timetable.chain WHERE chain_name = 'update_status_every_3sec';
    
    INSERT INTO timetable.chain (
        chain_name,
        run_at_minute,
        run_at_hour,
        run_at_day,
        run_at_month,
        run_at_day_of_week,
        max_instances,
        live,
        self_destruct,
        exclusive_execution
    ) VALUES (
        'update_status_every_3sec',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        1,
        TRUE,
        FALSE,
        TRUE
    ) RETURNING chain_id INTO v_chain_id;
    
    INSERT INTO timetable.task (
        chain_id,
        task_order,
        kind,
        command,
        run_as,
        ignore_error
    ) VALUES (
        v_chain_id,
        1,
        'SQL'::timetable.command_kind,
        'SELECT update_operation_status()',
        NULL,
        FALSE
    );
    
    RAISE NOTICE 'Created task: update_status_every_3sec (chain_id: %)', v_chain_id;
END $$;

-- Налаштування реплікації
-- ============================================================================

-- НА MASTER СЕРВЕРІ:
-- ---------------------------------------------------------------------------

-- 1. Налаштувати postgresql.conf на master:
/*
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
max_worker_processes = 10
*/

-- 2. Налаштувати pg_hba.conf для реплікації:
/*
# Для логічної реплікації
host    all             replication_user    <replica_ip>/32    scram-sha-256
host    replication     replication_user    <replica_ip>/32    scram-sha-256
*/

-- 3. Створити користувача для реплікації (на master):
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replication_user') THEN
        CREATE ROLE replication_user WITH 
            REPLICATION 
            LOGIN 
            PASSWORD 'ChangeMe_SecurePassword123!';
        
        RAISE NOTICE 'Created replication user';
    ELSE
        RAISE NOTICE 'Replication user already exists';
    END IF;
END $$;

-- 4. Надати права на таблицю (на master):
GRANT USAGE ON SCHEMA public TO replication_user;
GRANT SELECT ON operations_t1 TO replication_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO replication_user;

-- Для автоматичних прав на нові об'єкти:
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT SELECT ON TABLES TO replication_user;

-- 5. Створити публікацію (на master):
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'operations_pub') THEN
        CREATE PUBLICATION operations_pub FOR TABLE operations_t1;
        RAISE NOTICE 'Created publication: operations_pub';
    ELSE
        RAISE NOTICE 'Publication already exists';
    END IF;
END $$;

-- Перевірити публікацію:
-- SELECT * FROM pg_publication;
-- SELECT * FROM pg_publication_tables WHERE pubname = 'operations_pub';


-- НА REPLICA СЕРВЕРІ:
-- ---------------------------------------------------------------------------

-- 1. Створити таку ж структуру таблиці (виконати весь DDL з КРОКУ 1)
/*
-- Виконати на replica всі команди створення таблиць, індексів, функцій
-- АЛЕ НЕ виконувати генерацію даних (generate_test_data)
*/

-- 2. Створити підписку (на replica):
/*
-- ВАЖЛИВО: замініть параметри підключення на реальні
CREATE SUBSCRIPTION operations_sub
CONNECTION 'host=master_hostname port=5432 dbname=your_database user=replication_user password=ChangeMe_SecurePassword123!'
PUBLICATION operations_pub
WITH (
    copy_data = true,              -- копіювати існуючі дані
    create_slot = true,            -- створити replication slot
    enabled = true,                -- активувати одразу
    connect = true,                -- підключитись одразу
    slot_name = 'operations_slot', -- назва replication slot
    synchronous_commit = 'off'     -- для кращої продуктивності (можна 'on' для надійності)
);

-- Приклад для локального тесту (master і replica на одному сервері):
CREATE SUBSCRIPTION operations_sub
CONNECTION 'host=localhost port=5432 dbname=test_db user=replication_user password=ChangeMe_SecurePassword123!'
PUBLICATION operations_pub
WITH (copy_data = true, create_slot = true, enabled = true);
*/

-- 3. Перевірити статус реплікації (на replica):
/*
-- Статус підписки
SELECT * FROM pg_stat_subscription;

-- Деталі підписки
SELECT 
    subname,
    subenabled,
    subconninfo,
    subpublications
FROM pg_subscription;

-- Перевірити lag реплікації
SELECT 
    application_name,
    client_addr,
    state,
    sync_state,
    pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS lag_bytes
FROM pg_stat_replication;
*/

-- 4. Перевірити replication slots (на master):
/*
SELECT 
    slot_name,
    plugin,
    slot_type,
    database,
    active,
    active_pid,
    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
FROM pg_replication_slots;
*/
