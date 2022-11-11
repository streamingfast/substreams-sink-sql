create table pancakeswap.pair
(
    token_0   text   not null,
    token_1   text   not null,
    block     bigint not null,
    timestamp timestamp,
    name      text,
    id        text   not null
        constraint pair_pk
            primary key
);

alter table pancakeswap.pair
    owner to admin;


--> DatabaseChange
//substreams-postgres-sink run psql://unane@pword:localhost/database_v1?.... api-unstable.mainnet.streamingfast.io manifest.spkg db_out [<start>:<stop>]