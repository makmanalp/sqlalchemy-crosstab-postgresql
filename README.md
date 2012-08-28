sqlalchemy-crosstab-postgresql
==============================

New grammar for SQLAlchemy to handle the crosstab() tablefunc in Postgresql.
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

    select *
    from crosstab('select country, year, count from pop order by 1', 'select distinct year from pop order by 1')
    as derp (country text, y1 int, y2 int, y3 int, y4 int)

Where the first parameter is the input of form (key, thing_to_turn_into_columns, value) and the second is 

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
* For your category query, you can use a regular select distinct or you can just "fake it" by doing a select * from values (...).
* Your source sql *must* be ordered by 1, 2.
* Your category query must be ordered also.
* You have to pass in the queries as strings, which is a pain in the butt and causes issues when you need to escape things (e.g. a quote). Luckily, the [double dollar operator](http://www.postgresql.org/docs/current/interactive/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING) comes to the rescue.

