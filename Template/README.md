## Templates

The current template scheme is the following:
```
{company}_{platform}_{template type}_{object type}
```

What kind of template types do exist (so far in our architecture):
* block (reusable select statements for views or stored procedure)
* ddl (used for create statements for views and tables)
* procedure (used for stored procedures)
* utility (used for shared functionality among all templates)

### Utility

The utility section consists of 3 master templates:
* configuration
* elements
* snippets

In the **configuration template** are many default settings I reuse in other templates.

In the **elements template** are such things like:
* column lists
* where clauses
* joins

In the **snippets template** are just small recurring code to return strings and formulas.

Other utility templates are:
* procedure
* ddl

They have reusable patterns for template types

### DDL

These templates are useable in all objects. Instead of the default table creation it is possible to specify a view DDL instead. In that way it is not needed to create tables and stored procedures.

### Procedure

These templates are used for creating stored procedures

### Block

A block is in my case a SELECT statement for a specific purpose. Usually inserting data into a target table.

By designing a block I can reuse it in a view and a stored procedure at the same time. In that way I can select to create a view or a table with a stored procedure with the same outcome.

Think further and you are able to create virtual data warehouse.



