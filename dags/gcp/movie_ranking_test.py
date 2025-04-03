import internal_unit_testing

def test_dag_import():
    from . import movie_ranking

    internal_unit_testing.assert_has_valid_dag(movie_ranking)