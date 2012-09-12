sqlalchemy-crosstab-postgresql
==============================

New grammar for SQLAlchemy to make handling the crosstab() tablefunc in
Postgresql easy peasy. Jump down to the usage example if you're impatient.

This is a work-in-progress and not all that clean right now, but you're welcome
to bring in your fixes and patches!

Author: Mali Akmanalp

Thanks
------
* Michael Bayer of the sqlalchemy list for the original sample code.


What is crosstab?
-----------------

Crosstab, a.k.a. pivot, is best described by example. Let's say you have a table of population records:

<table>
    <tr>
        <th>Country</th>
        <th>Year</th>
        <th>Count</th>
    </tr>
    <tr>
        <td>India</td>
        <td>2009</td>
        <td>100</td>
    </tr>
    <tr>
        <td>India</td>
        <td>2010</td>
        <td>150</td>
    </tr>
    <tr>
        <td>India</td>
        <td>2011</td>
        <td>200</td>
    </tr>
    <tr>
        <td>Czechoslovakia</td>
        <td>2008</td>
        <td>200</td>
    </tr>
    <tr>
        <td>Czechoslovakia</td>
        <td>2010</td>
        <td>400</td>
    </tr>
</table>

and turning it into:

<table>
    <tr>
        <th>Country</th>
        <th>2008</th>
        <th>2009</th>
        <th>2010</th>
        <th>2011</th>
    </tr>
    <tr>
        <td>India</td>
        <td></td>
        <td>100</td>
        <td>150</td>
        <td>200</td>
    </tr>
    <tr>
        <td>Czechoslovakia</td>
        <td>200</td>
        <td></td>
        <td>400</td>
        <td></td>
    </tr>
</table>

Another way to think about it is that it takes tuples of (y, x, value) and
makes a new table where it puts value in location (x, y) e.g. 400 in (2010,
"Czechoslovakia").

So a sample query would look like this:

```sql
select *
from crosstab('select country, year, count from pop order by 1', 'select distinct year from pop order by 1')
as derp (country text, y1 int, y2 int, y3 int, y4 int)
```

where the first parameter is the input of form (key,
thing_to_turn_into_columns, value) (e.g. India, 2009, 100 etc.) and the second
is a list of possible column values (eg. 2008, 2009, 2010, 2011). The from
clause needs to declare the expected return types, which usually are the types
of (key, col1, col2, col3 ...) etc.

For more, read the [tablefunc docs](http://www.postgresql.org/docs/current/static/tablefunc.html).

Things I wish people had told me about crosstab
-----------------------------------------------
* The form crosstab(text sql) is useless if you have any empty fields in the
  resulting table (as in the example above), since the function doesn't place
  the data in intelligently, it just lumps it in so you'll end up with data in
  the wrong columns. See: [this](http://stackoverflow.com/questions/3002499/postgresql-crosstab-query#11751905)
* The fix for this is to use the form crosstab(text source_sql, text
  category_sql), where the second query must return a list of column names
  (e.g. "India", "Czechoslovakia"). These must be ordered also, otherwise the
  output gets wonky. Can't have dupes either.
* The easy-to-miss conclusion from the previous point is that it is up to you
  to make sure that when you define your return types, they should accurately
  depict what your input data contains. Following our example, if you claim in
  the return types that you only have 2 columns for 2008 and 2009, it will
  complain. So you need to know beforehand all the possible column values and
  use a "where year = 2008 or year = 2010 etc" to make other possible values
  not appear. The benefit of the latter call form with 2 parameters is that any
  extra column values are ignored and you don't have to deal with that.
* For your category query, you can use a regular select distinct or you can
  just "fake it" by doing a select * from values (...).
* Your source sql *must* be ordered by 1, 2.
* Your category query must be ordered also.
* You have to pass in the queries as strings, which is a pain in the butt and
  causes issues when you need to escape things (e.g. a quote). Luckily, the
  [double dollar operator](http://www.postgresql.org/docs/current/interactive/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING)
  comes to the rescue.

Usage
-----
crosstab.py handles most of the grossness for you so you can get work done. Check example.py for the full runnable code.

```python
crosstab_input = \
select([    raw.c.country,
            raw.c.year,
            raw.c.quantity])

categories = \
    select([distinct(raw.c.year)])


ret_types = Table('ct', m,  Column('country', Text),
                            Column('y1', Integer),
                            Column('y2', Integer),
                            Column('y3', Integer),
                            Column('y4', Integer),
                            )

q = select(['*']).select_from(crosstab(crosstab_input, ret_types, categories=categories))
```

generates the query:

```sql
SELECT * 
FROM crosstab(
    $$SELECT raw.country, raw.year, raw.quantity FROM raw ORDER BY 1,2$$,
    $$SELECT DISTINCT raw.year FROM raw ORDER BY 1$$)
AS ct(country TEXT, y1 INTEGER, y2 INTEGER, y3 INTEGER, y4 INTEGER)
```

crosstab.py also supplies row_total(), which allows you to sum a bunch of
fields in a row while ignoring NULLs and pretending they were 0s. Otherwise,
the NULLs would eat up the numbers and the total would be NULL. It does this
under the hood by calling coalesce(x, 0) on each field and then summing them.
This is meant to be used on the select part of a crosstab, such as:

```sql
select(['header', 'y1', 'y2', row_total('y1'. 'y2').label('sum')])\
    .select_from(crosstab(...))\
    .order_by('sum')
```

Which generates:

```sql
select header, y1, y2, coalesce(y1, 0) + coalesce(y2, 0) as sum from crosstab(...) order by sum
```
