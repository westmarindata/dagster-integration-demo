from dagster import op, get_dagster_logger

@op
def say_hello():
    get_dagster_logger().info("Hello!")
