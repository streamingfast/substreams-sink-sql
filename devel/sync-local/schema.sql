create table tokens
(
    id                text not null constraint token_pk primary key,
    contract_address  text,
    token_id          text,
    owner_address     text,
    created_block_num bigint,
    updated_block_num bigint
);

create table token_ownerships
(
    id                text not null constraint token_ownership_pk primary key,
    contract_address  text,
    token_id          text,
    owner_address     text,
    block_num         bigint,
    created_at        bigint,
    quantity          bigint
);


create table cursors
(
    id         text not null constraint cursor_pk primary key,
    cursor     text,
    block_num  bigint
);
