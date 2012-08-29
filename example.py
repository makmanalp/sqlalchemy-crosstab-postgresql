from crosstab import crosstab
from sqlalchemy import Table, Column, Text, Integer, MetaData, distinct, select

#create Engine and bind
from sqlalchemy import create_engine
engine = create_engine("postgresql+psycopg2:///dbname")
engine.echo = True
m = MetaData()
m.bind = engine

#Set up the sample source data
raw = Table('raw', m,   Column('country', Text),
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

#Define the input table
crosstab_input = \
select([    raw.c.country,
            raw.c.year,
            raw.c.quantity])

#Define the categories. For us, this is 2008, 2009, 2010 etc.
categories = \
    select([distinct(raw.c.year)])
#or you could fake the values like so:
#categories = select(['*']).select_from('(VALUES (2008), (2009), (2010), (2011)) x')


#Define the return row types. The fact that we're defining a table is a
#formality, it's just easier to do it that way. It won't ever get created.
ret_types = Table('ct', m,  Column('country', Text),
                            Column('y1', Integer),
                            Column('y2', Integer),
                            Column('y3', Integer),
                            Column('y4', Integer),
                            )

#Finally, the crosstab query itself. Has the input query, the category query and the return types.
q = select(['*']).select_from(crosstab(crosstab_input, ret_types, categories=categories))

#Ta-daaa!
print [x for x in engine.execute(q)]

#cleanup
raw.drop()

