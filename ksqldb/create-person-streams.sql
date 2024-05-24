SET 'auto.offset.reset'='earliest';

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
SELECT
    p.ROWKEY,
    p.first_name,
    p.middle_name,
    p.last_name,
    p.gender,
    p.ssn,
    p.date_of_birth,
    d.dod AS date_of_death,
    p.birth_location_name,
    p.birth_location_state,
    p.birth_location_country,
    p.birth_location_zipcode,
    d.dth_src_cd AS death_source_code,
    d.dth_certf_num AS death_certificate_num,
    d.dth_addr_zip5 AS death_address_zipcode
FROM DEATH_REPORTS_CDC_STREAM d
JOIN  PERSON_TABLE p
ON d.ROWKEY = p.ROWKEY
EMIT CHANGES;