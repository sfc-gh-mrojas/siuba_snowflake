# Siuba +. Snowflake

This is a toy project to test using the Siuba package with SnowPark procedures.

The project shows how some patching can be applied in order to allow using sqlalchemy from wihtin an Snowflake Snowpark procedure.

The method `common.create_sqlalchemy_engine` can be used to build a sqlalchemy engine.

The project will register a procedure called `test_siuba()` and run several methods and returns its output.

# Building the project

download the `snowflake_sqlalchemy` package.

For example:

```
wget https://files.pythonhosted.org/packages/py3/s/snowflake_sqlalchemy/snowflake_sqlalchemy-1.5.3-py3-none-any.whl
```

And upload it into an stage:

`snow stage copy snowflake_sqlalchemy-1.5.3-py3-none-any.whl @mystage --overwrite`

Build the package:

`snow snowpark build`

And deploy:

`snow snowpark deploy --replace`

# Testing

And you can test with:

`snow snowpark execute procedure "test_siuba()"`
