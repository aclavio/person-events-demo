SET 'auto.offset.reset'='earliest';

-- Create person info stream from CDC raw data
CREATE OR REPLACE STREAM person_cdc_stream WITH (
    KAFKA_TOPIC = 'pnumi.public.person_info-raw',
    VALUE_FORMAT = 'AVRO',
    KEY_FORMAT = 'AVRO'
);

-- Create person info stream from CDC raw data
CREATE OR REPLACE STREAM death_reports_cdc_stream WITH (
    KAFKA_TOPIC = 'pnumi.public.death_reports-raw',
    VALUE_FORMAT = 'AVRO',
    KEY_FORMAT = 'AVRO'
);

-- Create person info table for person entities
CREATE OR REPLACE TABLE person_table WITH (
    KAFKA_TOPIC = 'person.avro',
    VALUE_FORMAT = 'AVRO',
    KEY_FORMAT = 'AVRO'
);

-- Join the death alerts stream with the person info
CREATE STREAM person_death_alerts WITH (
    KAFKA_TOPIC = 'person.death.alert.avro',
    VALUE_FORMAT = 'AVRO',
    KEY_FORMAT = 'AVRO'
) AS
SELECT * FROM DEATH_REPORTS_CDC_STREAM 
JOIN  PERSON_TABLE 
ON DEATH_REPORTS_CDC_STREAM.ROWKEY = PERSON_TABLE.ROWKEY
EMIT CHANGES;

-- CREATE STREAM CLUB_STATUS_CHANGES WITH (
--     KAFKA_TOPIC='club-status-changes', 
--     VALUE_FORMAT='AVRO'
-- ) AS SELECT *
-- FROM CUSTOMER_CDC_STREAM 
-- WHERE (BEFORE->CLUB_STATUS <> AFTER->CLUB_STATUS)
-- EMIT CHANGES;

-- CREATE OR REPLACE TABLE customer_table WITH (
--     KAFKA_TOPIC = 'customer-data',
--     KEY_FORMAT = 'AVRO',
--     VALUE_FORMAT = 'AVRO'
-- ) AS SELECT 
--     ROWKEY,
--     LATEST_BY_OFFSET(AS_VALUE(ROWKEY->ID)) AS customer_id,
--     LATEST_BY_OFFSET(AFTER->FIRST_NAME) AS first_name,
--     LATEST_BY_OFFSET(AFTER->LAST_NAME) AS last_name,
--     LATEST_BY_OFFSET(AFTER->EMAIL) AS email,
--     LATEST_BY_OFFSET(AFTER->GENDER) AS gender,
--     LATEST_BY_OFFSET(AFTER->CLUB_STATUS) AS club_status,
--     LATEST_BY_OFFSET(AFTER->ZIP_CODE) AS zip_code,
--     LATEST_BY_OFFSET(AFTER->SSN) AS ssn
-- FROM CUSTOMER_CDC_STREAM
-- GROUP BY ROWKEY
-- EMIT CHANGES;

-- CREATE STREAM customer_changed_club_status WITH (
--     KAFKA_TOPIC = 'customer-changed-status',
--     VALUE_FORMAT = 'AVRO'
-- ) AS SELECT
--     ct.ROWKEY AS ROWKEY,
--     ct.customer_id,
--     ct.first_name,
--     ct.last_name,
--     ct.email,
--     ct.gender,
--     ct.club_status,
--     ct.zip_code,
--     ct.ssn,
--     CONCAT(csc.BEFORE->CLUB_STATUS, ' -> ', csc.AFTER->CLUB_STATUS) AS _metadata_change
-- FROM  CLUB_STATUS_CHANGES csc
-- JOIN  CUSTOMER_TABLE ct
-- ON ct.ROWKEY = csc.ROWKEY
-- EMIT CHANGES;
