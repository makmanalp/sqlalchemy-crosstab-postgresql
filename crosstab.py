from sqlalchemy.sql import FromClause, column
from sqlalchemy.ext.compiler import compiles

class crosstab(FromClause):
    def __init__(self, stmt, return_def, categories=None, auto_order=True):
        self.stmt = stmt
        self.return_name = return_def.name
        self.columns = return_def.columns
        self.categories = categories

        #Don't rely on the user to order their stuff
        if auto_order:
            self.stmt = self.stmt.order_by('1,2')
            if self.categories is not None:
                self.categories = self.categories.order_by('1')

    def _populate_column_collection(self):
        self._columns.update(
            column(name, type=type_)
            for name, type_ in self.names
        )

@compiles(crosstab)
def visit_element(element, compiler, **kw):
    if element.categories is not None:
        return """crosstab($$%s$$, $$%s$$) AS %s(%s)""" % (
            compiler.visit_select(element.stmt),
            compiler.visit_select(element.categories),
            element.return_name,
            ", ".join(
                "%s %s" % (c.name, compiler.visit_typeclause(c))
                for c in element.c
                )
            )
    else:
        return """crosstab($$%s$$) AS %s(%s)""" % (
            compiler.visit_select(element.stmt),
            element.return_name,
            ", ".join(
                "%s %s" % (c.name, compiler.visit_typeclause(c))
                for c in element.c
                )
            )
