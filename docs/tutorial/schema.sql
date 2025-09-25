create table block_meta
(
    id          text not null constraint block_meta_pk primary key,
    at          timestamp,
    number      bigint,
    hash        text,
    parent_hash text,
    timestamp   timestamp
);

create table cursors
(
    id         text not null constraint cursor_pk primary key,
    cursor     text,
    block_num  bigint,
    block_id   text
);