use async_sqlite::{JournalMode, PoolBuilder};
use log::info;
use qxsql::sql::{QxSqlApi, record_from_slice};
use rusqlite_migration::{M, Migrations};

use crate::{qxappsql::QxAppSql, state::EventData};

pub async fn migrate_db(db_file: &str, event_data: &EventData) -> anyhow::Result<()> {
    info!("Opening db {db_file} in journal mode: Wal");
    let pool = PoolBuilder::new()
                    .path(db_file)
                    .journal_mode(JournalMode::Wal);
    let pool = pool.open()
                    .await?;

    // Update the database schema, atomically
    pool.conn_mut(|conn| {
        match MIGRATIONS.to_latest(conn) {
            Ok(_) => Ok(()),
            Err(e) => panic!("{}", e),
        }
    }).await?;
    let qxsql = QxAppSql(pool);
    let config_entries = [
        ("event.name", event_data.name.clone()),
        ("event.date", event_data.date.format("%Y-%m-%d").to_string()),
        ("event.time", event_data.date.format("%H:%M:%S").to_string()),
    ];

    for (key, value) in config_entries {
        qxsql.exec("INSERT INTO config (ckey, cvalue) VALUES (:ckey, :cvalue)", Some(&record_from_slice(&[
            ("ckey", key.into()),
            ("cvalue", value.into()),
        ]))).await?;
    }
    qxsql.create_record("stages", &record_from_slice(&[
        ("startdatetime", event_data.date.clone().into()),
    ])).await?;
    info!("Migration of: {db_file} OK");

    Ok(())
}

const MIGRATIONS: Migrations = Migrations::from_slice(MIGRATION_ARRAY);

const MIGRATION_ARRAY: &[M] = &[
    M::up(
r#"
CREATE TABLE enumz (
    id integer PRIMARY KEY,
    groupname character varying,
    groupId character varying,
    pos integer,
    caption character varying,
    color character varying,
    value character varying,
    CONSTRAINT enumz_unique0 UNIQUE (groupname, groupid)
);

CREATE TABLE config (
    ckey character varying,
    cname character varying,
    cvalue character varying,
    ctype character varying,
    CONSTRAINT config_pkey PRIMARY KEY (ckey)
);

CREATE TABLE stages (
    id integer,
    startdatetime timestamp,
    useallmaps boolean NOT NULL DEFAULT 0,
    drawingconfig character varying,
    qxapitoken character varying,
    CONSTRAINT stages_pkey PRIMARY KEY (id)
);

CREATE TABLE courses (
    id integer PRIMARY KEY,
    name character varying,
    length integer,
    climb integer,
    note character varying,
    mapcount integer
);
CREATE INDEX courses_ix0 ON courses (name);

CREATE TABLE codes (
    id integer PRIMARY KEY,
    code integer,
    altcode integer,
    outoforder boolean NOT NULL DEFAULT 0,
    radio boolean NOT NULL DEFAULT 0,
    longitude double precision,
    latitude double precision,
    note character varying
);

CREATE TABLE coursecodes (
    id integer PRIMARY KEY,
    courseid integer,
    position integer,
    codeid integer,
    CONSTRAINT coursecodes_foreign0 FOREIGN KEY (courseid) REFERENCES courses (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT coursecodes_foreign1 FOREIGN KEY (codeid) REFERENCES codes (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE INDEX coursecodes_ix2 ON coursecodes (courseid, position);

CREATE TABLE classes (
    id integer PRIMARY KEY,
    name character varying,
    CONSTRAINT classes_unique0 UNIQUE (name)
);

CREATE TABLE classdefs (
    id integer PRIMARY KEY,
    classid integer,
    stageid integer,
    courseid integer,
    startslotindex integer NOT NULL DEFAULT -1,
    starttimemin integer,
    startintervalmin integer,
    vacantsbefore integer,
    vacantevery integer,
    vacantsafter integer,
    mapcount integer,
    resultscount integer,
    resultsprintts timestamp,
    laststarttimemin integer,
    drawlock boolean NOT NULL DEFAULT 0,
    relaystartnumber integer,
    relaylegcount integer,
    CONSTRAINT classdefs_foreign0 FOREIGN KEY (stageid) REFERENCES stages (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT classdefs_foreign1 FOREIGN KEY (classid) REFERENCES classes (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT classdefs_foreign2 FOREIGN KEY (courseid) REFERENCES courses (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE competitors (
    id integer PRIMARY KEY,
    startnumber integer,
    classid integer,
    firstname character varying,
    lastname character varying,
    registration character varying(10),
    iofid integer,
    licence character varying(1),
    club character varying,
    country character varying,
    siid integer,
    ranking integer,
    note character varying,
    importid integer,
    CONSTRAINT competitors_foreign0 FOREIGN KEY (classid) REFERENCES classes (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE INDEX competitors_ix1 ON competitors (importid);

CREATE TABLE runs (
    id integer PRIMARY KEY,
    competitorid integer,
    siid integer,
    stageid integer NOT NULL DEFAULT 1,
    leg integer,
    relayid integer,
    corridortime timestamp,
    checktimems integer,
    starttimems integer,
    finishtimems integer,
    penaltytimems integer,
    timems integer,
    isrunning boolean NOT NULL DEFAULT 1,
    disqualified boolean GENERATED ALWAYS AS (disqualifiedbyorganizer OR  mispunch OR  notstart OR  notfinish OR  badcheck OR  overtime OR notcompeting) STORED,
    disqualifiedbyorganizer boolean NOT NULL DEFAULT 0,
    notcompeting boolean NOT NULL DEFAULT 0,
    mispunch boolean NOT NULL DEFAULT 0,
    notstart boolean NOT NULL DEFAULT 0,
    notfinish boolean NOT NULL DEFAULT 0,
    badcheck boolean NOT NULL DEFAULT 0,
    overtime boolean NOT NULL DEFAULT 0,
    cardlent boolean NOT NULL DEFAULT 0,
    cardreturned boolean NOT NULL DEFAULT 0,
    importid integer,
    CONSTRAINT runs_foreign0 FOREIGN KEY (competitorid) REFERENCES competitors (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT runs_foreign1 FOREIGN KEY (stageid) REFERENCES stages (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE INDEX runs_ix2 ON runs (relayid, leg);
CREATE INDEX runs_ix3 ON runs (stageid, siid);

CREATE TABLE relays (
    id integer PRIMARY KEY,
    number integer,
    classid integer,
    club character varying,
    name character varying,
    note character varying,
    importid integer,
    isrunning boolean NOT NULL DEFAULT 1,
    CONSTRAINT relays_foreign0 FOREIGN KEY (classid) REFERENCES classes (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE INDEX relays_ix1 ON relays (club, name);
CREATE INDEX relays_ix2 ON relays (number);

CREATE TABLE runlaps (
    id integer PRIMARY KEY,
    runid integer,
    position integer,
    code integer,
    stptimems integer,
    laptimems integer
);
CREATE INDEX runlaps_ix0 ON runlaps (runid, position);
CREATE INDEX runlaps_ix1 ON runlaps (position, stptimems);
CREATE INDEX runlaps_ix2 ON runlaps (position, laptimems);

CREATE TABLE clubs (
    id integer PRIMARY KEY,
    name character varying,
    abbr character varying,
    importid integer
);
CREATE INDEX clubs_ix0 ON clubs (abbr);

CREATE TABLE registrations (
    id integer PRIMARY KEY,
    firstname character varying,
    lastname character varying,
    registration character varying(10),
    licence character varying(1),
    clubabbr character varying,
    country character varying,
    siid integer,
    importid integer
);
CREATE INDEX registrations_ix0 ON registrations (registration);

CREATE TABLE cards (
    id integer PRIMARY KEY,
    runid integer,
    runidassignts timestamp,
    runidassignerror character varying,
    stageid integer,
    stationnumber integer DEFAULT 0,
    siid integer,
    checktime integer,
    starttime integer,
    finishtime integer,
    punches character varying(65536),
    data character varying,
    readerconnectionid integer,
    printerconnectionid integer
);
CREATE INDEX cards_ix0 ON cards (readerconnectionid);
CREATE INDEX cards_ix1 ON cards (printerconnectionid);
CREATE INDEX cards_ix2 ON cards (stageid, siid);
CREATE INDEX cards_ix3 ON cards (runid);

CREATE TABLE punches (
    id integer PRIMARY KEY,
    code integer,
    siid integer,
    time integer,
    msec integer,
    stageid integer,
    runid integer,
    timems integer,
    runtimems integer
);
CREATE INDEX punches_ix0 ON punches (stageid, code);
CREATE INDEX punches_ix1 ON punches (runid);

CREATE TABLE stationsbackup (
    id integer PRIMARY KEY,
    stageid integer,
    stationnumber integer,
    siid integer,
    punchdatetime timestamp,
    carderr boolean,
    CONSTRAINT stationsbackup_unique0 UNIQUE (stageid, stationnumber, siid, punchdatetime)
);

CREATE TABLE lentcards (
    siid integer PRIMARY KEY,
    ignored boolean NOT NULL DEFAULT 0,
    note character varying
);

CREATE TABLE qxchanges (
    id integer PRIMARY KEY,
    stage_id integer,
    change_id integer,
    data_id integer,
    data_type character varying,
    data character varying,
    orig_data character varying,
    source character varying,
    user_id character varying,
    status character varying,
    status_message character varying,
    created timestamp,
    lock_number integer,
    CONSTRAINT qxchanges_unique0 UNIQUE (stage_id, change_id)
);

------------------------------------;
-- insert initial data;
------------------------------------;
INSERT INTO config (ckey, cname, cvalue, ctype) VALUES
('db.version', 'Data version', '30301', 'int');
"#,
    ),
];
