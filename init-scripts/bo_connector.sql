

CREATE SCHEMA IF NOT EXISTS bo_connectors;

SET search_path TO bo_connectors;


---
--- drop tables
---


DROP TABLE IF EXISTS destinations;
DROP TABLE IF EXISTS sources;
DROP TABLE IF EXISTS task_runs;


CREATE TABLE destinations (
	id text NOT NULL,
	name text NOT NULL,
	app_configuration text NOT NULL,
	created_at timestamp,
	UNIQUE (name),
	CONSTRAINT pk_destinations  PRIMARY KEY (id)
);

CREATE TABLE  sources (
	id text NOT NULL,
	name text NOT NULL,
	app_configuration text NOT NULL,
	created_at timestamp,
	UNIQUE (name),
	CONSTRAINT pk_sources  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS task_runs (
	id text NOT NULL,
	name text NOT NULL,
    app_name text NOT NULL,
	reference_id text NOT NULL,
	status int2 NOT NULL,
	type int2 NOT NULL,
    is_cdc_data BOOLEAN NOT NULL,
	created_at timestamp,
	completed_at timestamp,
    stopped_at timestamp,
	runned_at timestamp,
	occurred_at timestamp,
	error_message text NULL,
	row_version text NOT NULL,
	UNIQUE (id, status),
	CONSTRAINT pk_task_runs  PRIMARY KEY (id)
);