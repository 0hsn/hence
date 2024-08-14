"""Test @task()"""

from icecream import ic

from hence import TaskConfig, Utils, _context, task, run_tasks, CTX_TI_BASE


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

        task_obj = _context.context_get(CTX_TI_BASE, "sample_task")

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
            fc = Utils.get_task(run_it)
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

        fc = Utils.get_task(run_sequence[0])
        assert fc.function.__name__ == fc.result

        fc = Utils.get_task(run_sequence[1])
        assert "task_1 Hello, World" == fc.result

    @staticmethod
    def test_run_tasks_pass_with_replace_title():
        """test run tasks pass with replace title"""

        @task(title="task_1-{fn_task_key}")
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

        fc = Utils.get_task(run_sequence[0])
        assert f"task_1-{run_sequence[0]}" == fc.title

        _, run_id = run_sequence[1].split(".", 1)
        fc = Utils.get_task(run_sequence[1])
        assert f"task_2-{run_id}" == fc.title

        seq_id, _ = run_sequence[2].split(".", 1)
        fc = Utils.get_task(run_sequence[2])
        assert f"task_3-{seq_id}" == fc.title

    @staticmethod
    def test_run_tasks_pass_with_multiple_run_tasks():
        """test run tasks pass with multiple run tasks"""

        @task(title="task_1-{fn_task_key}")
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
            ],
        )

        fc = Utils.get_task(run_sequence[0])
        assert f"task_1-{run_sequence[0]}" == fc.title

        _, run_id = run_sequence[1].split(".", 1)
        fc = Utils.get_task(run_sequence[1])
        assert f"task_2-{run_id}" == fc.title

        seq_id, _ = run_sequence[2].split(".", 1)
        fc = Utils.get_task(run_sequence[2])
        assert f"task_3-{seq_id}" == fc.title

        @task(title="task_4-{fn_task_key}")
        def task_4(**kwargs):
            return task_4.__name__

        @task(title="task_5-{fn_run_id}")
        def task_5(**kwargs):
            return task_5.__name__

        @task(title="task_6-{fn_seq_id}")
        def task_6(**kwargs):
            return task_6.__name__

        run_sequence = run_tasks(
            [
                (task_4, {}),
                (task_5, {}),
                (task_6, {}),
            ]
        )

        fc = Utils.get_task(run_sequence[0])
        assert f"task_4-{run_sequence[0]}" == fc.title

        _, run_id = run_sequence[1].split(".", 1)
        fc = Utils.get_task(run_sequence[1])
        assert f"task_5-{run_id}" == fc.title

        seq_id, _ = run_sequence[2].split(".", 1)
        fc = Utils.get_task(run_sequence[2])
        assert f"task_6-{seq_id}" == fc.title
