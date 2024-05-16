#!/bin/sh

echo "Create Reference table and import data:"
docker exec -i postgres psql -U myuser -d postgres << EOF
create table ZIPCODE_REF (
        zip_code VARCHAR(6) PRIMARY KEY,
        official_usps_city_name VARCHAR(50),
        official_usps_state_code VARCHAR(2),
        official_state_name VARCHAR(50),
        zcta VARCHAR(50),
        zcta_parent VARCHAR(50),
        population VARCHAR(50),
        density VARCHAR(50),
        primary_official_county_code VARCHAR(50),
        primary_official_county_name VARCHAR(50),
        county_weights VARCHAR(200),
        official_county_name VARCHAR(100),
        official_county_code VARCHAR(100),
        imprecise VARCHAR(50),
        military VARCHAR(50),
        timezone VARCHAR(50),
        geo_point VARCHAR(50)
);

COPY ZIPCODE_REF(zip_code, official_usps_city_name, official_usps_state_code, official_state_name, zcta, zcta_parent, population, density, primary_official_county_code, primary_official_county_name, county_weights, official_county_name, official_county_code, imprecise, military, timezone, geo_point)
FROM '/tmp/reference-data/georef-united-states-of-america-zc-point@public.csv'
DELIMITER ';'
CSV HEADER;
EOF

echo "Create CUSTOMERS table:"
docker exec -i postgres psql -U myuser -d postgres << EOF
create table CUSTOMERS (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(50),
        gender VARCHAR(50),
        club_status VARCHAR(20),
        comments VARCHAR(90),
        zip_code VARCHAR(6),
        create_ts timestamp DEFAULT CURRENT_TIMESTAMP ,
        update_ts timestamp DEFAULT CURRENT_TIMESTAMP
);


-- Courtesy of https://techblog.covermymeds.com/databases/on-update-timestamps-mysql-vs-postgres/
CREATE FUNCTION update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS \$\$
  BEGIN
    NEW.update_ts = NOW();
    RETURN NEW;
  END;
\$\$;

CREATE TRIGGER t1_updated_at_modtime BEFORE UPDATE ON CUSTOMERS FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Rica', 'Blaisdell', 'rblaisdell0@rambler.ru', 'Female', 'bronze', 'Universal optimal hierarchy', '62233');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Ruthie', 'Brockherst', 'rbrockherst1@ow.ly', 'Female', 'platinum', 'Reverse-engineered tangible interface', '79529');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Mariejeanne', 'Cocci', 'mcocci2@techcrunch.com', 'Female', 'bronze', 'Multi-tiered bandwidth-monitored capability', '32228');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Hashim', 'Rumke', 'hrumke3@sohu.com', 'Male', 'platinum', 'Self-enabling 24/7 firmware', '1104');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Hansiain', 'Coda', 'hcoda4@senate.gov', 'Male', 'platinum', 'Centralized full-range approach', '3904');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Robinet', 'Leheude', 'rleheude5@reddit.com', 'Female', 'platinum', 'Virtual upward-trending definition', '13494');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Fay', 'Huc', 'fhuc6@quantcast.com', 'Female', 'bronze', 'Operative composite capacity', '15477');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Patti', 'Rosten', 'prosten7@ihg.com', 'Female', 'silver', 'Integrated bandwidth-monitored instruction set', '2808');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Even', 'Tinham', 'etinham8@facebook.com', 'Male', 'silver', 'Virtual full-range info-mediaries', '18438');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Brena', 'Tollerton', 'btollerton9@furl.net', 'Female', 'silver', 'Diverse tangible methodology', '20171');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Alexandro', 'Peeke-Vout', 'apeekevouta@freewebs.com', 'Male', 'gold', 'Ameliorated value-added orchestration', '20553');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Sheryl', 'Hackwell', 'shackwellb@paginegialle.it', 'Female', 'gold', 'Self-enabling global parallelism', '23701');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Laney', 'Toopin', 'ltoopinc@icio.us', 'Female', 'platinum', 'Phased coherent alliance', '28722');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Isabelita', 'Talboy', 'italboyd@imageshack.us', 'Female', 'gold', 'Cloned transitional synergy', '32204');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Rodrique', 'Silverton', 'rsilvertone@umn.edu', 'Male', 'gold', 'Re-engineered static application', '33983');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Clair', 'Vardy', 'cvardyf@reverbnation.com', 'Male', 'bronze', 'Expanded bottom-line Graphical User Interface', '34746');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Brianna', 'Paradise', 'bparadiseg@nifty.com', 'Female', 'bronze', 'Open-source global toolset', '46256');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Waldon', 'Keddey', 'wkeddeyh@weather.com', 'Male', 'gold', 'Business-focused multi-state functionalities', '47272');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Josiah', 'Brockett', 'jbrocketti@com.com', 'Male', 'gold', 'Realigned didactic info-mediaries', '84302');
insert into CUSTOMERS (first_name, last_name, email, gender, club_status, comments, zip_code) values ('Anselma', 'Rook', 'arookj@europa.eu', 'Female', 'gold', 'Cross-group 24/7 application', '39532');

EOF

echo "Show content of CUSTOMERS table:"
docker exec -i postgres psql -U myuser -d postgres << EOF
SELECT * FROM CUSTOMERS;
EOF

# this adds the full "before" context to CDC messages
echo "Set update mode of CUSTOMERS table:"
docker exec -i postgres psql -U myuser -d postgres << EOF
ALTER TABLE customers REPLICA IDENTITY FULL;
EOF

echo "Creating Debezium PostgreSQL source connector - customers"
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @- << EOF
{
    "name": "debezium-postgres-cdc-source",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "myuser",
        "database.password": "mypassword",
        "database.dbname" : "postgres",
        "topic.prefix": "asgard",
        "table.include.list": "public.customers",

        "key.converter" : "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": "http://schema-registry:8081",
        "value.converter" : "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",

        "transforms": "addTopicSuffix",
        "transforms.addTopicSuffix.type":"org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.addTopicSuffix.regex":"(.*)",
        "transforms.addTopicSuffix.replacement": "\$1-raw",

        "_comment:": "remove _ to use ExtractNewRecordState smt",
        "_transforms": "unwrap,addTopicSuffix",
        "_transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
    }
}
EOF
echo ""

echo "Creating Debezium PostgreSQL source connector - reference data"
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @- << EOF
{
    "name": "debezium-postgres-cdc-source-refdata",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "myuser",
        "database.password": "mypassword",
        "database.dbname" : "postgres",
        "slot.name": "debezium2",
        "topic.prefix": "reference",
        "table.include.list": "public.zipcode_ref",

        "key.converter" : "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": "http://schema-registry:8081",
        "value.converter" : "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",

        "_comment:": "remove _ to use ExtractNewRecordState smt",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
    }
}
EOF
echo ""

echo "create the streaming apps"
docker exec -i ksqldb-cli ksql http://ksqldb-server:8088 << EOF
SET 'auto.offset.reset'='earliest';

CREATE OR REPLACE STREAM customer_cdc_stream WITH (
    KAFKA_TOPIC = 'asgard.public.customers-raw',
    VALUE_FORMAT = 'AVRO',
    KEY_FORMAT = 'AVRO'
);

CREATE STREAM CLUB_STATUS_CHANGES WITH (
    KAFKA_TOPIC='club-status-changes', 
    VALUE_FORMAT='AVRO'
) AS SELECT *
FROM CUSTOMER_CDC_STREAM 
WHERE (BEFORE->CLUB_STATUS <> AFTER->CLUB_STATUS)
EMIT CHANGES;

CREATE OR REPLACE TABLE customer_table WITH (
    KAFKA_TOPIC = 'customer-data',
    KEY_FORMAT = 'AVRO',
    VALUE_FORMAT = 'AVRO'
) AS SELECT 
    ROWKEY,
    LATEST_BY_OFFSET(AS_VALUE(ROWKEY->ID)) AS customer_id,
    LATEST_BY_OFFSET(AFTER->FIRST_NAME) AS first_name,
    LATEST_BY_OFFSET(AFTER->LAST_NAME) AS last_name,
    LATEST_BY_OFFSET(AFTER->EMAIL) AS email,
    LATEST_BY_OFFSET(AFTER->GENDER) AS gender,
    LATEST_BY_OFFSET(AFTER->CLUB_STATUS) AS club_status
FROM CUSTOMER_CDC_STREAM
GROUP BY ROWKEY
EMIT CHANGES;

CREATE STREAM customer_changed_club_status WITH (
    KAFKA_TOPIC = 'customer-changed-status',
    VALUE_FORMAT = 'AVRO'
) AS SELECT
    ct.ROWKEY AS ROWKEY,
    ct.customer_id,
    ct.first_name,
    ct.last_name,
    ct.email,
    ct.gender,
    ct.club_status,
    CONCAT(csc.BEFORE->CLUB_STATUS, ' -> ', csc.AFTER->CLUB_STATUS) AS `_metadata_change`
FROM  CLUB_STATUS_CHANGES csc
JOIN  CUSTOMER_TABLE ct
ON ct.ROWKEY = csc.ROWKEY
EMIT CHANGES;
EOF

echo "DONE!"