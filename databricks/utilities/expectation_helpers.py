from pyspark import pipelines as dp

def apply_expectations(expectations):
    def decorator(func):
        return dp.expect_all(expectations)(func)
    return decorator