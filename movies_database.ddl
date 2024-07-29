CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    file_path TEXT,
    rating FLOAT CHECK(rating > 0 AND rating < 100),
    type TEXT not null,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre(
id uuid PRIMARY KEY,
name TEXT NOT NULL UNIQUE,
description TEXT,
created timestamp with time zone,
modified timestamp with time zone);

CREATE TABLE IF NOT EXISTS content.genre_film_work(
id uuid PRIMARY KEY,
film_work_id uuid NOT NULL REFERENCES content.film_work (id) ON DELETE CASCADE,
genre_id uuid NOT NULL REFERENCES content.genre (id) ON DELETE CASCADE,
created timestamp with time zone);

CREATE TABLE IF NOT EXISTS content.person(
id uuid PRIMARY KEY,
full_name TEXT NOT NULL,
created timestamp with time zone,
modified timestamp with time zone);

CREATE TABLE IF NOT EXISTS content.person_film_work(
id uuid PRIMARY KEY,
film_work_id uuid NOT NULL REFERENCES content.film_work (id) ON DELETE CASCADE,
person_id uuid NOT NULL REFERENCES content.person (id) ON DELETE CASCADE,
role TEXT NOT NULL,
created timestamp with time zone);


CREATE UNIQUE INDEX IF NOT EXISTS idx_person_film_work_unique
ON content.person_film_work (film_work_id, person_id, role);

CREATE UNIQUE INDEX IF NOT EXISTS idx_genre_film_work_unique
ON content.genre_film_work (film_work_id, genre_id);

ALTER ROLE app SET search_path TO content,public;
