from hr import app, utils


def test_hr(capture_stdout):
    """ Tests hr function that acts as a Spark job. """

    spark_session = utils.get_spark_context("HR Spark test app - calculates grade and bonus information.")
    app.run(spark_session)

    assert "James,Smith" in capture_stdout["stdout"]
    assert "Anna,Rose" in capture_stdout["stdout"]
    assert "Robert,Williams" in capture_stdout["stdout"]
