from __future__ import print_function
import os
import pytest
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import CompileError
from sqlalchemy import Table, Column, Text, Integer, MetaData, distinct, select
from sqlalchemy import create_engine, func
from crosstab import crosstab


# Yes, this is a lot of fixtures. I think the modularity is useful.

@pytest.fixture(scope="module", params=('envfile.txt',))
def pgengine(request):
    """Create a sqlalchemy engine for a PostgreSQL DB."""
    envFile = request.param
    if os.environ.get('DBCONNECTION', None) is None:
        with open(envFile) as envVars:
            for line in envVars:
                var, val = line.split('=', 1)
                var = var.strip()
                val = val.strip()
                os.environ[var] = val
    dbConnectionString = os.environ['DBCONNECTION']
    return create_engine(dbConnectionString, echo=True)


@pytest.fixture(scope="module")
def sqliteEngine():
    """Create a sqlalchemy engine for a memory-only SQLite instance."""
    return create_engine('sqlite:///:memory:', echo=True)


@pytest.fixture(scope="function")
def pgmetadata(pgengine):
    """Creates a metadata object for the engine for SQLA Core Tests."""
    meta = MetaData(bind=pgengine)
    yield meta
    meta.drop_all()


@pytest.fixture(scope="function")
def tableraw(pgmetadata):
    #Set up the sample source data
    raw = Table('raw', pgmetadata,
                Column('country', Text),
                Column('year', Integer),
                Column('quantity', Integer),
                Column('unrelated_field', Text))
    data = [
            ('India', 2009, 100, "foo"),
            ('India', 2010, 150, "foo"),
            ('India', 2011, 200, "foo"),
            ('Czechoslovakia', 2008, 200, "foo"),
            ('Czechoslovakia', 2010, 400, "foo")
            ]
    raw.create()
    for line in data:
        raw.insert().values(line).execute()
    yield raw
    raw.drop()


# ---------- ORM Fixtures -------------------------------------------------


@pytest.fixture(scope="module")
def OrmBase():
    return declarative_base()


@pytest.fixture(scope="module")
def ormCensus(OrmBase):
    class OrmCensus(OrmBase):
        __tablename__ = 'ormcensus'
        id = Column(Integer, primary_key=True)
        country = Column(Text)
        year = Column(Integer)
        quantity = Column(Integer)
        unrelated_field = Column(Text)

    return OrmCensus


@pytest.fixture(scope="module")
def pgsessionclass(pgengine, OrmBase, ormCensus):
    """Creates a Session class, which can then generate sessions."""
    OrmBase.metadata.create_all(pgengine)
    return sessionmaker(bind=pgengine)


@pytest.fixture(scope="function")
def pgsession(pgsessionclass):
    """Create an ORM Session for PostgreSQL."""
    sess = pgsessionclass()
    yield sess
    sess.rollback()


@pytest.fixture(scope="function")
def ormCensusData(ormCensus, pgsession):
    pgsession.add_all([
        ormCensus(country='India', year=2009, quantity=100, unrelated_field='foo'),
        ormCensus(country='India', year=2010, quantity=150, unrelated_field='foo'),
        ormCensus(country='India', year=2011, quantity=200, unrelated_field='foo'),
        ormCensus(country='Czechoslovakia', year=2008, quantity=200, unrelated_field='foo'),
        ormCensus(country='Czechoslovakia', year=2010, quantity=400, unrelated_field='foo')
    ])


# -------------------------------------------------------------------------
# And now for the tests.


def test_coreReturnAll(pgengine, pgmetadata, tableraw):
    """Test the simple case of crosstabing all of the data in the table."""
    raw = tableraw
    # Define the input table
    crosstab_input = \
    select([    raw.c.country,
                raw.c.year,
                raw.c.quantity])

    # Define the categories. For us, this is 2008, 2009, 2010 etc.
    categories = \
        select([distinct(raw.c.year)])

    # Define return columns. Table is an easy way to do that.
    ret_types = Table('ct', pgmetadata,
                      Column('country', Text),
                      Column('y1', Integer),
                      Column('y2', Integer),
                      Column('y3', Integer),
                      Column('y4', Integer),
                      )

    # Finally, the crosstab query itself.
    q = select(['*']).select_from(crosstab(crosstab_input, ret_types,
                                           categories=categories))

    assert [tuple(x) for x in pgengine.execute(q)] == [
        ('Czechoslovakia', 200, None, 400, None),
        ('India', None, 100, 150, 200)
        ]

def test_coreReturnSome(pgengine, pgmetadata, tableraw):
    """Test the case of crossstabing a query with a where clause."""
    raw = tableraw
    # Define the input table
    crosstab_input = \
    select([    raw.c.country,
                raw.c.year,
                raw.c.quantity]).where(raw.c.country == 'India')

    # Define the categories. For us, this is 2008, 2009, 2010 etc.
    categories = \
        select([distinct(raw.c.year)])

    # Define return columns. Table is an easy way to do that.
    ret_types = Table('ct', pgmetadata,
                      Column('country', Text),
                      Column('y1', Integer),
                      Column('y2', Integer),
                      Column('y3', Integer),
                      Column('y4', Integer),
                      )

    # Finally, the crosstab query itself.
    q = select(['*']).select_from(crosstab(crosstab_input, ret_types,
                                           categories=categories))

    assert [tuple(x) for x in pgengine.execute(q)] == [
        ('India', None, 100, 150, 200)
        ]


def test_coreAggregation(pgengine, pgmetadata, tableraw):
    """Test the case of crossstabing with a sum() and a where clause."""
    raw = tableraw
    # Define the input table
    crosstab_input = \
    select([    raw.c.unrelated_field,
                raw.c.year,
                func.sum(raw.c.quantity).label('quantity')]
           ).where(
               raw.c.unrelated_field == 'foo'
           ).group_by(
               raw.c.unrelated_field,
               raw.c.year
           )

    # Define the categories. For us, this is 2008, 2009, 2010 etc.
    categories = \
        select([distinct(raw.c.year)])

    # Define return columns. Table is an easy way to do that.
    ret_types = Table('ct', pgmetadata,
                      Column('country', Text),
                      Column('y1', Integer),
                      Column('y2', Integer),
                      Column('y3', Integer),
                      Column('y4', Integer),
                      )

    # Finally, the crosstab query itself.
    q = select(['*']).select_from(crosstab(crosstab_input, ret_types,
                                           categories=categories))

    assert [tuple(x) for x in pgengine.execute(q)] == [
        ('foo', 200, 100, 550, 200)
        ]


def test_breaksOnSqlite(sqliteEngine):
    meta = MetaData(bind=sqliteEngine)

    raw = Table('raw', meta,
                Column('country', Text),
                Column('year', Integer),
                Column('quantity', Integer),
                Column('unrelated_field', Text))
    data = [
            ('India', 2009, 100, "foo"),
            ('India', 2010, 150, "foo"),
            ('India', 2011, 200, "foo"),
            ('Czechoslovakia', 2008, 200, "foo"),
            ('Czechoslovakia', 2010, 400, "foo")
            ]
    raw.create()
    for line in data:
        raw.insert().values(line).execute()

    # Define the input table
    crosstab_input = \
    select([    raw.c.country,
                raw.c.year,
                raw.c.quantity]).where(raw.c.country == 'India')

    # Define the categories. For us, this is 2008, 2009, 2010 etc.
    categories = \
        select([distinct(raw.c.year)])

    # Define return columns. Table is an easy way to do that.
    ret_types = Table('ct', meta,
                      Column('country', Text),
                      Column('y1', Integer),
                      Column('y2', Integer),
                      Column('y3', Integer),
                      Column('y4', Integer),
                      )

    # Finally, the crosstab query itself.
    q = select(['*']).select_from(crosstab(crosstab_input, ret_types,
                                           categories=categories))

    with pytest.raises(Exception):
        sqliteEngine.execute(q)

    raw.drop()
    meta.drop_all()


def test_coreReturnBySelect(pgengine, pgmetadata, tableraw):
    """Test the simple case of crosstabing all of the data in the table."""
    raw = tableraw
    # Define the input table
    crosstab_input = \
    select([    raw.c.country,
                raw.c.year,
                raw.c.quantity])

    # Define the categories. For us, this is 2008, 2009, 2010 etc.
    categories = \
        select([distinct(raw.c.year)])

    # Define return columns. Table is an easy way to do that.
    ret_types = select([
        raw.c.country.label('country'),
        raw.c.quantity.label('y1'),
        raw.c.quantity.label('y2'),
        raw.c.quantity.label('y3'),
        raw.c.quantity.label('y4')
    ])

    # Finally, the crosstab query itself.
    q = select(['*']).select_from(crosstab(crosstab_input, ret_types,
                                           categories=categories))

    assert [tuple(x) for x in pgengine.execute(q)] == [
        ('Czechoslovakia', 200, None, 400, None),
        ('India', None, 100, 150, 200)
        ]


def test_coreReturnByTuple(pgengine, pgmetadata, tableraw):
    """Test the simple case of crosstabing all of the data in the table."""
    raw = tableraw
    # Define the input table
    crosstab_input = \
    select([    raw.c.country,
                raw.c.year,
                raw.c.quantity])

    # Define the categories. For us, this is 2008, 2009, 2010 etc.
    categories = \
        select([distinct(raw.c.year)])

    # Define return columns. Table is an easy way to do that.
    ret_types = (
        Column('country', Text),
        Column('y1', Integer),
        Column('y2', Integer),
        Column('y3', Integer),
        Column('y4', Integer)
    )

    # Finally, the crosstab query itself.
    q = select(['*']).select_from(crosstab(crosstab_input, ret_types,
                                           categories=categories))

    assert [tuple(x) for x in pgengine.execute(q)] == [
        ('Czechoslovakia', 200, None, 400, None),
        ('India', None, 100, 150, 200)
        ]


# --------------------- ORM Tests -------------------------------------
@pytest.mark.usefixtures("ormCensusData")
def test_ormReturnAll(ormCensus, pgsession):
    """Test that you can combine crosstab with ORM Queries."""
    # Define the input table
    crosstab_input = pgsession.query(ormCensus.country, ormCensus.year,
                                     ormCensus.quantity)

    # Define the categories. For us, this is 2008, 2009, 2010 etc.
    categories = pgsession.query(ormCensus.year).group_by(ormCensus.year)

    # Define return columns. Table is an easy way to do that.
    ret_types = (
        Column('country', Text),
        Column('y1', Integer),
        Column('y2', Integer),
        Column('y3', Integer),
        Column('y4', Integer)
    )

    # Finally, the crosstab query itself.
    q = pgsession.query(*ret_types).select_from(
        crosstab(crosstab_input, ret_types, categories=categories))

    assert [tuple(x) for x in q.all()] == [
        ('Czechoslovakia', 200, None, 400, None),
        ('India', None, 100, 150, 200)
        ]


@pytest.mark.usefixtures("ormCensusData")
def test_ormReturnSome(ormCensus, pgsession):
    """Test that you can combine crosstab with ORM Queries."""
    # Define the input table
    crosstab_input = pgsession.query(
        ormCensus.country,
        ormCensus.year,
        ormCensus.quantity
        ).filter(ormCensus.country == 'India')

    # Define the categories. For us, this is 2008, 2009, 2010 etc.
    categories = pgsession.query(ormCensus.year).group_by(ormCensus.year)

    # Define return columns. Table is an easy way to do that.
    ret_types = (
        Column('country', Text),
        Column('y1', Integer),
        Column('y2', Integer),
        Column('y3', Integer),
        Column('y4', Integer)
    )

    # Finally, the crosstab query itself.
    q = pgsession.query(*ret_types).select_from(
        crosstab(crosstab_input, ret_types, categories=categories))

    assert [tuple(x) for x in q.all()] == [
        ('India', None, 100, 150, 200)
        ]


@pytest.mark.usefixtures("ormCensusData")
def test_ormReturnAggregate(ormCensus, pgsession):
    """Test that you can combine crosstab with ORM Queries."""
    # Define the input table
    crosstab_input = pgsession.query(
        ormCensus.unrelated_field,
        ormCensus.year,
        func.sum(ormCensus.quantity)
    ).filter(
        ormCensus.unrelated_field == 'foo'
    ).group_by(
        ormCensus.unrelated_field,
        ormCensus.year
    )

    # Define the categories. For us, this is 2008, 2009, 2010 etc.
    categories = pgsession.query(ormCensus.year).group_by(ormCensus.year)

    # Define return columns. Table is an easy way to do that.
    ret_types = (
        Column('country', Text),
        Column('y1', Integer),
        Column('y2', Integer),
        Column('y3', Integer),
        Column('y4', Integer)
    )

    # Finally, the crosstab query itself.
    q = pgsession.query(*ret_types).select_from(
        crosstab(crosstab_input, ret_types, categories=categories))

    assert [tuple(x) for x in q.all()] == [
        ('foo', 200, 100, 550, 200)
        ]
