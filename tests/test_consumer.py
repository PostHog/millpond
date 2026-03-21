from millpond.consumer import compute_assignment


class TestComputeAssignment:
    def test_even_distribution(self):
        assert compute_assignment(8, 2, 0) == [0, 2, 4, 6]
        assert compute_assignment(8, 2, 1) == [1, 3, 5, 7]

    def test_single_replica(self):
        assert compute_assignment(4, 1, 0) == [0, 1, 2, 3]

    def test_more_replicas_than_partitions(self):
        assert compute_assignment(2, 4, 0) == [0]
        assert compute_assignment(2, 4, 1) == [1]
        assert compute_assignment(2, 4, 2) == []
        assert compute_assignment(2, 4, 3) == []

    def test_uneven_distribution(self):
        # 10 partitions, 3 replicas
        assert compute_assignment(10, 3, 0) == [0, 3, 6, 9]
        assert compute_assignment(10, 3, 1) == [1, 4, 7]
        assert compute_assignment(10, 3, 2) == [2, 5, 8]
