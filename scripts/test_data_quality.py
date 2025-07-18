def run_quality_tests(df):
    from pyspark.sql.functions import col, count, when
    errors = {}

    null_check = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    errors['nulls'] = null_check.collect()[0].asDict()

    duplicate_email = df.groupBy("EMAIL").count().filter("count > 1").count()
    errors['duplicate_emails'] = duplicate_email

    salary_range_violations = df.filter("SALARY < 30000 OR SALARY > 200000").count()
    errors['salary_range'] = salary_range_violations

    future_hire_date = df.filter("HIRE_DATE > current_date()").count()
    errors['future_hire_date'] = future_hire_date

    return errors
