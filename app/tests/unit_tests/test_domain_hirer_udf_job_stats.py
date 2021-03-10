from app.domain.hirer import udf_job_stats

# testing "_is_entry_level_ad"
def test_is_entry_level_ad():
    assert udf_job_stats._is_entry_level_ad("") == 0
    assert udf_job_stats._is_entry_level_ad(None) == 0
    assert udf_job_stats._is_entry_level_ad("Junior Executive") == 1


# testing "_is_engineering_ad"
def test_is_engineering_ad():
    assert udf_job_stats._is_engineering_ad("") == 0
    assert udf_job_stats._is_engineering_ad(None) == 0
    assert udf_job_stats._is_engineering_ad("Software Engineer") == 1

