## Templates

The current template scheme is the following:
```
{company}_{platform}_{template type}_{object type}
```

What kind of template types do exist (so far in our architecture):
* ddl (used for create statements for views and tables)
* procedure (used for stored procedures)
* utility (used for shared functionality among all templates)

### Utility

The utility section consists of 2 master templates:
* elements
* snippets

In the **elements template** are such things like:
* column lists
* where clauses
* joins

While in the **snippets template** are just small recurring code to return strings and formulas.

### Procedure

The procedure template type starts with the outline of a stored procedure and recurring elements. Actually it is also an 
utility template but for better organization and because it can only be used for procedure templates it lies here.

## Notes

From a default environment I changed some stuff:
* All date data types are of datetime2(7) 
* I use sysdatetime() to get insert_date or update_date
* My major system column for load dates is dss_load_datetime

Modify accordingly to get it working.
