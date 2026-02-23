from DSLInterpreter import DSLInterpreter
import polars as pl
import yaml

yml = pl.DataFrame(
        {
            'age': [25, None, 35, 1000, 28],
            'salary': [50_000, 60_000, None, 75_000, 55_000],
            'to' : ["Alice", "BOB", "  Charlie", None, "dave"],
            'city': ["London", "paris", "BERLIN", "Madrid", None]
        }
)

intp = DSLInterpreter(yml)

commands = ["fill null using forward", "transform columns age, salary using log"] 
result = intp.run(yaml.dump(commands))
if result is not None:
    print(result.head(2))
else:
    print("failure")
