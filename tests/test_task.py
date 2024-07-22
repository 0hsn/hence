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
    def test_run_tasks_pass():
        """test run tasks pass"""

        @task(title="1")
        def task_1(**kwargs):
            return task_1.__name__

        @task(title="2")
        def task_2(**kwargs):
            return task_2.__name__

        run_sequence = run_tasks(
            [
                (task_1, {}),
                (task_2, {}),
            ]
        )

        for run_it in run_sequence:
            fc = hence_config.task(run_it)
            assert fc.function.__name__ == fc.result

    @staticmethod
    def test_run_tasks_pass_for_same_task():
        """test run tasks pass for same task"""

        @task(title="task one")
        def task_1(**kwargs):
            return_stmt = task_1.__name__

            if len(kwargs):
                return_stmt += f' {kwargs["var1"]}, {kwargs["var2"]}'

            return return_stmt

        run_sequence = run_tasks(
            [
                (task_1, {}),
                (task_1, {"var1": "Hello", "var2": "World"}),
            ]
        )

        fc = hence_config.task(run_sequence[0])
        assert fc.function.__name__ == fc.result

        fc = hence_config.task(run_sequence[1])
        assert "task_1 Hello, World" == fc.result

    @staticmethod
    def test_run_tasks_pass_with_replace_title():
        """test run tasks pass with replace title"""

        @task(title="task_1-{fn_key}")
        def task_1(**kwargs):
            return task_1.__name__

        @task(title="task_2-{fn_run_id}")
        def task_2(**kwargs):
            return task_2.__name__

        @task(title="task_3-{fn_seq_id}")
        def task_3(**kwargs):
            return task_2.__name__

        run_sequence = run_tasks(
            [
                (task_1, {}),
                (task_2, {}),
                (task_3, {}),
            ]
        )

        fc = hence_config.task(run_sequence[0])
        assert "task_1-task_1.0" == fc.title

        fc = hence_config.task(run_sequence[1])
        assert "task_2-" == fc.title

        fc = hence_config.task(run_sequence[2])
        assert "task_3-2" == fc.title
