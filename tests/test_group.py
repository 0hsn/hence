"""Test module for group()"""

import pytest

from hence import Utils, group, task, run_group, _context as hc, CTX_GR_BASE


class TestGroup:
    """TestGroup"""

    @staticmethod
    def test_group_fail_when_same_group_name_used():
        """test group fail when same group name used"""

        ag = group("a-group")

        @ag
        @task()
        def sample_task(**kwargs):
            return kwargs

        with pytest.raises(ValueError):
            group("a-group")

        hc._setup_context()

    @staticmethod
    def test_group_pass_when_other_group_name_used():
        """test group pass when other group name used"""

        ag = group("a-group")

        @ag
        @task()
        def sample_task(**kwargs):
            return kwargs

        bg = group("b-group")

        @bg
        @task()
        def sample_task_1(**kwargs):
            return kwargs

        groups: dict = hc.context_get(CTX_GR_BASE)
        hc._setup_context()

        assert ["a-group", "b-group"] == list(groups.keys())

    @staticmethod
    def test_group_pass():
        """test group pass"""

        GRP_NAME = "a-group"

        ag = group(GRP_NAME)

        @ag
        @task()
        def sample_task(**kwargs):
            del kwargs["_META_"]
            return kwargs

        @ag
        @task(title="SampleTask1")
        def sample_task_1(**kwargs):
            del kwargs["_META_"]
            return kwargs

        task_ids = run_group(
            GRP_NAME,
            [
                {"a": 12},
                {"b": 123},
            ],
        )

        _task_d1 = Utils.get_task(task_ids[0])
        assert {"a": 12} == _task_d1.result

        _task_d1 = Utils.get_task(task_ids[1])
        assert {"b": 123} == _task_d1.result

        hc._setup_context()

    @staticmethod
    def test_group_pass_when_overlapping_group():
        """test group pass when overlapping group"""

        GRP_NAME_A = "a-group"
        ag = group(GRP_NAME_A)

        @ag
        @task()
        def sample_task(**kwargs):
            del kwargs["_META_"]
            return kwargs

        @ag
        @task(title="SampleTask1")
        def sample_task_1(**kwargs):
            del kwargs["_META_"]
            return kwargs

        task_ids = run_group(
            GRP_NAME_A,
            [
                {"a": 12},
                {"b": 123},
            ],
        )

        _task_d1 = Utils.get_task(task_ids[0])
        assert {"a": 12} == _task_d1.result

        _task_d1 = Utils.get_task(task_ids[1])
        assert {"b": 123} == _task_d1.result

        GRP_NAME_B = "b-group"
        bg = group(GRP_NAME_B)

        @bg
        @task()
        def sample_task_b(**kwargs):
            del kwargs["_META_"]
            return kwargs

        @bg
        @task(title="SampleTask1")
        def sample_task_b_1(**kwargs):
            del kwargs["_META_"]
            return kwargs

        task_ids = run_group(
            GRP_NAME_B,
            [
                {"a": 12},
                {"b": 123},
            ],
        )

        _task_d1 = Utils.get_task(task_ids[0])
        assert {"a": 12} == _task_d1.result

        _task_d1 = Utils.get_task(task_ids[1])
        assert {"b": 123} == _task_d1.result

        hc._setup_context()
