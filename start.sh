#!/bin/sh

echo "Create Reference tables and import data:"
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

create table DEATH_SOURCE_CODE_REF (
        code VARCHAR(2) PRIMARY KEY,
        death_source VARCHAR(100)
);

COPY DEATH_SOURCE_CODE_REF(code, death_source)
FROM '/tmp/reference-data/death-source-code.csv'
DELIMITER ';'
CSV HEADER;
EOF

echo "Create PERSON_INFO table:"
docker exec -i postgres psql -U myuser -d postgres << EOF
create table PERSON_INFO (
        cossn VARCHAR(11) PRIMARY KEY,
        dob DATE,
        dod DATE,
        sex_cd VARCHAR(1),
        fnm VARCHAR(254),
        mnm VARCHAR(254),
        lnm VARCHAR(254),
        brthloclt_nm VARCHAR(254),
        brthst_cd VARCHAR(2),
        brthcntry_cd VARCHAR(2),
        brthloclt_cd VARCHAR(6),
        brth_addr_loc_cd VARCHAR(1),
        create_ts timestamp DEFAULT CURRENT_TIMESTAMP,
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

CREATE TRIGGER t1_updated_at_modtime BEFORE UPDATE ON PERSON_INFO FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

COPY PERSON_INFO(cossn, dob, dod, sex_cd, fnm, mnm, lnm, brthloclt_nm, brthst_cd, brthcntry_cd, brthloclt_cd, brth_addr_loc_cd)
FROM '/tmp/reference-data/person-info.csv'
DELIMITER ','
CSV HEADER;

EOF

echo "Show content of PERSON_INFO table:"
docker exec -i postgres psql -U myuser -d postgres << EOF
SELECT * FROM PERSON_INFO;
EOF

# this adds the full "before" context to CDC messages
echo "Set update mode of PERSON_INFO table:"
docker exec -i postgres psql -U myuser -d postgres << EOF
ALTER TABLE PERSON_INFO REPLICA IDENTITY FULL;
EOF

echo "Create DEATH_REPORTS table:"
docker exec -i postgres psql -U myuser -d postgres << EOF
create table DEATH_REPORTS (
        cossn VARCHAR(11) PRIMARY KEY,
        prcg_dt DATE,
        dod DATE,
        dth_src_cd VARCHAR(2),
        dth_certf_num VARCHAR(26),
        dth_addr_zip5 VARCHAR(5),
        sex VARCHAR(1),
        clnt_fnm VARCHAR(15),
        clnt_mnm VARCHAR(15),
        clnt_lnm VARCHAR(20)
);

COPY DEATH_REPORTS(cossn, prcg_dt, dod, dth_src_cd, dth_certf_num, dth_addr_zip5, sex, clnt_fnm, clnt_mnm, clnt_lnm)
FROM '/tmp/reference-data/death-reports.csv'
DELIMITER ','
CSV HEADER;

EOF

echo "Creating Debezium PostgreSQL source connector - persons"
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
        "topic.prefix": "pnumi",
        "table.include.list": "public.person_info,public.death_reports",

        "key.converter" : "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": "http://schema-registry:8081",
        "value.converter" : "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
        "time.precision.mode": "connect",

        "transforms": "unwrap,convertDates,addTopicSuffix",

        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",

        "transforms.convertDates.type": "org.apache.kafka.connect.transforms.TimestampConverter\$Value",
        "transforms.convertDates.target.type": "unix",
        "transforms.convertDates.field": "dob",
        "transforms.convertDates.field": "dod",
        "transforms.convertDates.field": "prcg_dt",

        "transforms.addTopicSuffix.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.addTopicSuffix.regex": "(.*)",
        "transforms.addTopicSuffix.replacement": "\$1-raw"
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
        "table.include.list": "public.zipcode_ref,public.death_source_code_ref",

        "key.converter" : "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schema.registry.url": "http://schema-registry:8081",
        "key.converter.schemas.enable": "false",
        "value.converter" : "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
        "value.converter.schemas.enable": "false",

        "_comment:": "remove _ to use ExtractNewRecordState smt",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
    }
}
EOF
echo ""

echo "Creating Redis sink connector - reference data"
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @- << EOF
{
  "name": "redis-sink-json",
  "config": {
    "connector.class": "com.github.jcustenborder.kafka.connect.redis.RedisSinkConnector",
    "tasks.max": "1",
    "topics": "reference.public.zipcode_ref,reference.public.death_source_code_ref",
    "redis.hosts": "redis:6379",
    "redis.command": "JSONSET",
    "_key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.schemas.enable": "false",
    "_transforms": "Cast,Drop",
    "_transforms.Cast.type": "org.apache.kafka.connect.transforms.Cast\$Key",
    "_transforms.Cast.spec": "string",
    "_transforms.Drop.type": "org.apache.kafka.connect.transforms.ReplaceField\$Value",
    "_transforms.Drop.exclude": "county_weights"
  }
}
EOF
echo ""

echo "creating topics"
docker exec -i broker /bin/kafka-topics --bootstrap-server localhost:9092 --create --topic person.avro --partitions 1 --config "cleanup.policy=compact"

echo "create the streaming apps"
docker exec -i ksqldb-cli ksql -f /tmp/ksqldb/create-ksql-streams.sql -- http://ksqldb-server:8088

echo "DONE!"