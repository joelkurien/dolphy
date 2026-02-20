# dolphy csv cleaning dsl

a YAML-based domain specific language for cleaning csv data through a web interface. write cleaning commands in plain english ( no need for shift pressing except brackets) .

---

## what is this

Dolphy lets you clean csv files by writing simple commands in a yaml list. you type the commands in a yaml interface, upload your csv, and get a clean file back. no python, no pandas, no code - ehh maybe some code.

---

## key design principle

the dsl is designed so that if you are lazy with python or sql then try out this cool joke yaml language. Write it in whatever case you want I do not care, or may be I might care if some bugs popup which I will have to fix.

---

## getting started

commands are written as a yaml list and pasted into the web interface:

```yaml
- filter where age gt 30
- drop any null rows
- fill null in column salary using mean
- normalise columns name using lower
```

upload your csv, paste your commands, and click run.

---

## command reference

### filtering rows

filter rows using conditions. supports word-based operators so you never need symbols.

```yaml
- filter where age gt 30
- filter where salary lt 70000
- filter where name eq charlie
- filter where age gte 18
- filter where score lte 100
- filter where status neq inactive
```

**operators:**

| word | symbol | meaning |
|------|--------|---------|
| gt | > | greater than |
| lt | < | less than |
| gte | >= | greater than or equal |
| lte | <= | less than or equal |
| eq | == | equals |
| neq | != | not equal |

**combining conditions:**

```yaml
- filter where age gt 30 and salary lt 70000
- filter where city eq london or city eq paris
- filter where age gt 18 and name eq charlie and city eq berlin
```

**using brackets for complex conditions:**

brackets are the only time you need the shift key.

```yaml
- filter where (age gt 30 and salary lt 70000) or (city eq berlin and name eq charlie)
```

---

### dropping null and nan rows

```yaml
- drop any null rows
- drop all null rows
- drop any nan rows
- drop null and nan rows
- drop rows where age is null
- drop rows where salary is nan
- drop rows where age, salary is null
```

---

### filling null values

```yaml
- fill null using mean
- fill null using median
- fill null using mode
- fill null using forward
- fill null using backward
- fill null in column age using mean
- fill null in column salary, age using median
- fill null in column status using active
```

for a custom fill value (like a specific word or number), just write it after `using`.

---

### transformations

apply mathematical transformations to numeric columns.

```yaml
- transform columns age using log
- transform columns salary using sqrt
- transform columns score using square
- transform columns age using reciprocal
- transform columns age using yeojohnson
- transform columns age, salary using log inplace
```

without `inplace`, a new column is created (e.g. `age_log`). with `inplace`, the original column is replaced.

---

### normalisation

**numeric normalisation:**

```yaml
- normalise columns age using z-score
- normalise columns salary using min-max
- normalise columns score using robust
```

**string normalisation:**

```yaml
- normalise columns name using lower
- normalise columns city using upper
- normalise columns name using strip
- normalise columns category using label encoding
```

---

### renaming columns

```yaml
- rename age to years
- rename first name, last name to fname, lname
```

column names with spaces are supported — just write them as-is and separate multiple columns with commas.

---

## using the web app

1. go to the web app url
2. upload your csv file
3. write your commands as a yaml list in the editor
4. click **run**
5. preview the cleaned data
6. download the result

---

## full example

given a csv with columns: `age`, `salary`, `name`, `city`

```yaml
- drop any null rows
- filter where age gt 18 and salary gt 30000
- filter where (city eq london or city eq berlin) and name neq unknown
- fill null in column salary using mean
- normalise columns name using lower
- normalise columns name using strip
- normalise columns age using z-score
- transform columns salary using log inplace
- rename age to years
```

---

## notes

- commands are case-insensitive — `FILTER WHERE` and `filter where` both work
- column names are case-sensitive and must match the csv header exactly
- string values in filter conditions do not need quotes
- use commas to separate multiple columns in a single command
- the only time you need the shift key is for brackets in complex filter conditions

---

## tech stack

- **polars** — fast dataframe processing with lazy evaluation
- **pydantic** — column validation before execution
- **pyyaml** — yaml command parsing
- **python ast** — filter expression parsing
