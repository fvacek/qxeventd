use async_sqlite::{JournalMode, PoolBuilder};
use log::info;
use rusqlite_migration::{M, Migrations};

pub async fn migrate_db(db_file: &str) -> anyhow::Result<()> {
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
    info!("Migration of: {db_file} OK");

    Ok(())
}

const MIGRATIONS: Migrations = Migrations::from_slice(MIGRATION_ARRAY);

const MIGRATION_ARRAY: &[M] = &[
    M::up(
r#"
CREATE TABLE enumz (
    id integer PRIMARY KEY,
    groupName character varying,
    groupId character varying,
    pos integer,
    caption character varying,
    color character varying,
    value character varying,
    CONSTRAINT enumz_unique0 UNIQUE (groupName, groupId)
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
    startDateTime timestamp,
    useAllMaps boolean NOT NULL DEFAULT 0,
    drawingConfig character varying,
    qxApiToken character varying,
    CONSTRAINT stages_pkey PRIMARY KEY (id)
);

CREATE TABLE courses (
    id integer PRIMARY KEY,
    name character varying,
    length integer,
    climb integer,
    note character varying,
    mapCount integer
);
CREATE INDEX courses_ix0 ON courses (name);

CREATE TABLE codes (
    id integer PRIMARY KEY,
    code integer,
    altCode integer,
    outOfOrder boolean NOT NULL DEFAULT 0,
    radio boolean NOT NULL DEFAULT 0,
    longitude double precision,
    latitude double precision,
    note character varying
);

CREATE TABLE coursecodes (
    id integer PRIMARY KEY,
    courseId integer,
    position integer,
    codeId integer,
    CONSTRAINT coursecodes_foreign0 FOREIGN KEY (courseId) REFERENCES courses (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT coursecodes_foreign1 FOREIGN KEY (codeId) REFERENCES codes (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE INDEX coursecodes_ix2 ON coursecodes (courseId, position);

CREATE TABLE classes (
    id integer PRIMARY KEY,
    name character varying,
    CONSTRAINT classes_unique0 UNIQUE (name)
);

CREATE TABLE classdefs (
    id integer PRIMARY KEY,
    classId integer,
    stageId integer,
    courseId integer,
    startSlotIndex integer NOT NULL DEFAULT -1,
    startTimeMin integer,
    startIntervalMin integer,
    vacantsBefore integer,
    vacantEvery integer,
    vacantsAfter integer,
    mapCount integer,
    resultsCount integer,
    resultsPrintTS timestamp,
    lastStartTimeMin integer,
    drawLock boolean NOT NULL DEFAULT 0,
    relayStartNumber integer,
    relayLegCount integer,
    CONSTRAINT classdefs_foreign0 FOREIGN KEY (stageId) REFERENCES stages (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT classdefs_foreign1 FOREIGN KEY (classId) REFERENCES classes (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT classdefs_foreign2 FOREIGN KEY (courseId) REFERENCES courses (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE competitors (
    id integer PRIMARY KEY,
    startNumber integer,
    classId integer,
    firstName character varying,
    lastName character varying,
    registration character varying(10),
    iofId integer,
    licence character varying(1),
    club character varying,
    country character varying,
    siId integer,
    ranking integer,
    note character varying,
    importId integer,
    CONSTRAINT competitors_foreign0 FOREIGN KEY (classId) REFERENCES classes (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE INDEX competitors_ix1 ON competitors (importId);

CREATE TABLE runs (
    id integer PRIMARY KEY,
    competitorId integer,
    siId integer,
    stageId integer NOT NULL DEFAULT 1,
    leg integer,
    relayId integer,
    corridorTime timestamp,
    checkTimeMs integer,
    startTimeMs integer,
    finishTimeMs integer,
    penaltyTimeMs integer,
    timeMs integer,
    isRunning boolean NOT NULL DEFAULT 1,
    disqualified boolean GENERATED ALWAYS AS (disqualifiedByOrganizer OR  misPunch OR  notStart OR  notFinish OR  badCheck OR  overTime OR notCompeting) STORED,
    disqualifiedByOrganizer boolean NOT NULL DEFAULT 0,
    notCompeting boolean NOT NULL DEFAULT 0,
    misPunch boolean NOT NULL DEFAULT 0,
    notStart boolean NOT NULL DEFAULT 0,
    notFinish boolean NOT NULL DEFAULT 0,
    badCheck boolean NOT NULL DEFAULT 0,
    overTime boolean NOT NULL DEFAULT 0,
    cardLent boolean NOT NULL DEFAULT 0,
    cardReturned boolean NOT NULL DEFAULT 0,
    importId integer,
    CONSTRAINT runs_foreign0 FOREIGN KEY (competitorId) REFERENCES competitors (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT runs_foreign1 FOREIGN KEY (stageId) REFERENCES stages (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE INDEX runs_ix2 ON runs (relayId, leg);
CREATE INDEX runs_ix3 ON runs (stageId, siId);

CREATE TABLE relays (
    id integer PRIMARY KEY,
    number integer,
    classId integer,
    club character varying,
    name character varying,
    note character varying,
    importId integer,
    isRunning boolean NOT NULL DEFAULT 1,
    CONSTRAINT relays_foreign0 FOREIGN KEY (classId) REFERENCES classes (id) ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE INDEX relays_ix1 ON relays (club, name);
CREATE INDEX relays_ix2 ON relays (number);

CREATE TABLE runlaps (
    id integer PRIMARY KEY,
    runId integer,
    position integer,
    code integer,
    stpTimeMs integer,
    lapTimeMs integer
);
CREATE INDEX runlaps_ix0 ON runlaps (runId, position);
CREATE INDEX runlaps_ix1 ON runlaps (position, stpTimeMs);
CREATE INDEX runlaps_ix2 ON runlaps (position, lapTimeMs);

CREATE TABLE clubs (
    id integer PRIMARY KEY,
    name character varying,
    abbr character varying,
    importId integer
);
CREATE INDEX clubs_ix0 ON clubs (abbr);

CREATE TABLE registrations (
    id integer PRIMARY KEY,
    firstName character varying,
    lastName character varying,
    registration character varying(10),
    licence character varying(1),
    clubAbbr character varying,
    country character varying,
    siId integer,
    importId integer
);
CREATE INDEX registrations_ix0 ON registrations (registration);

CREATE TABLE cards (
    id integer PRIMARY KEY,
    runId integer,
    runIdAssignTS timestamp,
    runIdAssignError character varying,
    stageId integer,
    stationNumber integer DEFAULT 0,
    siId integer,
    checkTime integer,
    startTime integer,
    finishTime integer,
    punches character varying(65536),
    data character varying,
    readerConnectionId integer,
    printerConnectionId integer
);
CREATE INDEX cards_ix0 ON cards (readerConnectionId);
CREATE INDEX cards_ix1 ON cards (printerConnectionId);
CREATE INDEX cards_ix2 ON cards (stageId, siId);
CREATE INDEX cards_ix3 ON cards (runId);

CREATE TABLE punches (
    id integer PRIMARY KEY,
    code integer,
    siId integer,
    time integer,
    msec integer,
    stageId integer,
    runId integer,
    timeMs integer,
    runTimeMs integer
);
CREATE INDEX punches_ix0 ON punches (stageId, code);
CREATE INDEX punches_ix1 ON punches (runId);

CREATE TABLE stationsbackup (
    id integer PRIMARY KEY,
    stageId integer,
    stationNumber integer,
    siId integer,
    punchDateTime timestamp,
    cardErr boolean,
    CONSTRAINT stationsbackup_unique0 UNIQUE (stageId, stationNumber, siId, punchDateTime)
);

CREATE TABLE lentcards (
    siId integer PRIMARY KEY,
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
