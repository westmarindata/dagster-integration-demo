from dagster import get_dagster_logger, op


@op
def say_hello():
    get_dagster_logger().info("Hello!")
