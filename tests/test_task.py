"""Test @task()"""

from icecream import ic
from hence import hence_config, task, run_tasks, CTX_TI_BASE


class TestTaskDecorator:
    """TestTaskDecorator"""

    @staticmethod
    def test_task_create():
        """test task create"""

        @task()
        def sample_task(**kwargs):
            return sample_task.__name__

        assert sample_task.__name__ == sample_task()

    @staticmethod
    def test_task_create_fail_with_no_kwargs():
        """test task create fail with no kwargs"""

        try:

            @task()
            def sample_task():
                return sample_task.__name__

            sample_task()

        except Exception as te:
            assert isinstance(te, TypeError)

    @staticmethod
    def test_task_add_title():
        """test task add title"""

        @task(title="sample_task title")
        def sample_task(**kwargs):
            """sample_task"""

            return "tag1" if "tag" in kwargs else sample_task.__name__

        sample_task(tag="tag1")

        task_obj = hence_config.context_search(CTX_TI_BASE, "sample_task")

        assert task_obj == "sample_task title"


class TestRunTask:
    """TestRunTask"""

    @staticmethod
    def test_run_tasks_pass_without_tag():
        """test run tasks pass"""

        @task(title="1")
        def task_1(**kwargs):
            return task_1.__name__

        @task(title="2")
        def task_2(**kwargs):
            return task_2.__name__

        run_tasks(
            [
                (task_1, {}),
                (task_2, {}),
            ]
        )

        assert task_1.__name__ == hence_config.task(task_1.__name__).title
        assert task_2.__name__ == hence_config.task(task_2.__name__).title

    @staticmethod
    def test_run_tasks_pass_with_tag():
        """test run tasks pass"""

        @task(title="1")
        def task_1(**kwargs):
            return task_1.__name__

        @task(title="2")
        def task_2(**kwargs):
            return task_2.__name__

        run_tasks(
            [
                (task_1, {}, "11111"),
                (task_2, {}, "22222"),
            ]
        )

        assert task_1.__name__ == hence_config.task(task_1.__name__ + ".11111").title
        assert task_2.__name__ == hence_config.task(task_2.__name__ + ".22222").title

    @staticmethod
    def test_run_tasks_pass_with_replace_title():
        """test run tasks pass with replace title"""

        @task(title="task_1-{fn_key}")
        def task_1(**kwargs):
            return task_1.__name__

        @task(title="task_2-{fn_run_id}")
        def task_2(**kwargs):
            return task_2.__name__

        run_tasks(
            [
                (task_1, {}, "1"),
                (task_2, {}, "2"),
            ]
        )

        assert (
            f"{task_1.__name__}-{task_1.__name__}.1"
            == hence_config.task(task_1.__name__ + ".1").title
        )
        assert f"{task_2.__name__}-2" == hence_config.task(task_2.__name__ + ".2").title
