
Can Only Handle 2 levels of operations atm. Branchs, which are made using a data source and dimension tables. And a fact table, using branches and dimension tables.

What is a Branch?

A Branch is a pathway for a platform for getting raw data from a source and managing the manipulations with dimension tables. They also can potentially write these intermediary result tables or save the results in RAM for later dependencies.

# Return Dictionary
{
'name': NAME,
# flat file or SQL Table
  'origin_path': PATH
  'origin_table': TABLE_NAME
# flat file or SQL Table or None
  'dest_path': PATH
  'dest_table': TABLE_NAME
'df': DATAFRAME
}

Auto Should Read/Write, that's it
Branch accepts a DataFrame and it return a DataFrame
