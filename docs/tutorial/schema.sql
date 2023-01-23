create table block_data
(
    id          text not null constraint block_meta_pk primary key,
    version      integer,
    at        text,
    number        text,
    hash        text,
    parent_hash        text,
    timestamp        text
);

create table cursors
(
    id         text not null constraint cursor_pk primary key,
    cursor     text,
    block_num  bigint,
    block_id   text
);