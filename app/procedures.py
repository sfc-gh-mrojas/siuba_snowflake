from __future__ import annotations

import sys

from common import print_hello, create_sqlalchemy_engine
from snowflake.snowpark import Session
from siuba import *

from siuba.dply.verbs import collect, distinct, show_query
from siuba.sql.verbs import LazyTbl
from siuba.sql.dply.vector import dense_rank


def hello_procedure(session: Session, name: str) -> str:
    return print_hello(name)

def test_dbcooper(session: Session) -> str:
    import contextlib
    import os
    with open('/tmp/output.txt', 'w') as f:
        with contextlib.redirect_stdout(f):
            from dbcooper import DbCooper
            engine = create_sqlalchemy_engine(session)
            _, opts = engine.dialect.create_connect_args(engine.url)
            db_name, schema_name = opts.get("database"), opts.get("schema")
            dbc = DbCooper(engine)
            print(dbc.PUBLIC_ADDRESSES)
            print(dbc.list())
            print("done")
    out = open('/tmp/output.txt','r').read()
    print(out)
    return out


def test_siuba(session: Session) -> str:
    import contextlib
    import os
    with open('/tmp/output.txt', 'w') as f:
        with contextlib.redirect_stdout(f):
            from sqlalchemy import sql
            from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
            from sqlalchemy import create_engine
            engine = create_sqlalchemy_engine(session)
            metadata = MetaData()
            users = Table('users', metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String),
                Column('fullname', String),
            )


            addresses = Table('addresses', metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', None, ForeignKey('users.id')),
            Column('email_address', String, nullable=False)
            )

            metadata.drop_all(engine)
            metadata.create_all(engine)

            conn = engine.connect()

            ins = users.insert().values(name='jack', fullname='Jack Jones')
            result = conn.execute(ins)


            ins = users.insert()
            conn.execute(ins, id=2, name='wendy', fullname='Wendy Williams')


            conn.execute(addresses.insert(), [
            {'user_id': 1, 'email_address' : 'jack@yahoo.com'},
            {'user_id': 1, 'email_address' : 'jack@msn.com'},
            {'user_id': 2, 'email_address' : 'www@www.org'},
            {'user_id': 2, 'email_address' : 'wendy@aol.com'},
            ])



            from sqlalchemy import sql

            tbl_addresses = LazyTbl(conn, addresses)
            tbl_users = LazyTbl(conn, users)

            #tbl_addresses >> mutate(_, num = dense_rank(_.id)) >> show_query(_)
            q = (tbl_addresses
                >> group_by("user_id")
                >> mutate(num = dense_rank(_.id))
                >> filter(
                    _.id > _.id.min(),
                    _.email_address.str.startswith("jack")
                    )
                >> ungroup()
                >> show_query(simplify = True)
                >> collect()
            )
            print(q)
            q = (tbl_addresses
                >> mutate(rank = dense_rank(_.id) + 1)
                >> show_query()
            )
            print(q)
            q = (tbl_addresses
                >> group_by("user_id")
                >> mutate(rank = _.id > dense_rank(_.id) + 1)
                >> show_query()
            )
            print(q)
            # rename and first mutate in same query,
            # second mutate is outer query (since uses to prev col)
            q = (tbl_addresses
                >> select(_.email == _.email_address)
                >> mutate(is_mikey = _.email.str.startswith("mikey"), mikey2 = _.is_mikey)
                >> show_query()
            )
            print(q)
            q = (tbl_addresses
                >> filter(_.id > 1)
                >> show_query()
            )
            print(q)
            q = (tbl_addresses
                >> group_by("user_id")
                >> filter(_.id > 1)
                >> show_query()
            )
            print(q)
            q = (tbl_addresses
                >> group_by("user_id")
                >> filter(dense_rank(_.id) > 1)
                >> show_query()
                >> collect()
            )
            print(q)
            print("====== SIUBA TESTS COMPLETED======")
    return open('/tmp/output.txt','r').read()


# For local debugging
# Beware you may need to type-convert arguments if you add input parameters
if __name__ == "__main__":
    import sys
    sys._xoptions['snowflake_import_directory'] = "/Users/mrojas/siuba_snowflake"
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as session:
        test_dbcooper(session)  # type: ignore
