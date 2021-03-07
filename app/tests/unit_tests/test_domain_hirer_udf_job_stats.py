from app.domain.hirer import udf_job_stats


# testing "_is_entry_level_ad"
def test_is_entry_level_ad_emptystring():
    # Arrange
    sample_input = ""
    # Act
    output: int = udf_job_stats._is_entry_level_ad(sample_input)
    print(output)
    # Assert
    assert output == 0


def test_is_entry_level_ad_none():
    # Arrange
    sample_input = None
    # Act
    output: int = udf_job_stats._is_entry_level_ad(sample_input)
    print(output)
    # Assert
    assert output == 0


def test_is_entry_level_ad_string():
    # Arrange
    sample_input = "Junior Executive"
    # Act
    output: int = udf_job_stats._is_entry_level_ad(sample_input)
    print(output)
    # Assert
    assert output == 1


# testing "_is_engineering_ad"
def test_is_engineering_ad_emptystring():
    # Arrange
    sample_input = ""
    # Act
    output: int = udf_job_stats._is_engineering_ad(sample_input)
    print(output)
    # Assert
    assert output == 0


def test_is_engineering_ad_none():
    # Arrange
    sample_input = None
    # Act
    output: int = udf_job_stats._is_engineering_ad(sample_input)
    print(output)
    # Assert
    assert output == 0


def test_is_engineering_ad_string():
    # Arrange
    sample_input = "Junior Engineer"
    # Act
    output: int = udf_job_stats._is_engineering_ad(sample_input)
    print(output)
    # Assert
    assert output == 1

