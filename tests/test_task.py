"""Test @task()"""

from hence import hence_config, task, CTX_FN_BASE


def test_task_create():
    """test task create"""

    @task()
    def sample_task(**kwargs):
        return sample_task.__name__

    assert sample_task.__name__ == sample_task()


def test_task_create_fail_with_no_kwargs():
    """test task create fail with no kwargs"""

    try:

        @task()
        def sample_task():
            return sample_task.__name__

        sample_task()

    except Exception as te:
        assert isinstance(te, TypeError)


def test_task_add_title():
    """test task add title"""

    @task(title="sample_task title")
    def sample_task(**kwargs):
        """sample_task"""

        return "tag1" if "tag" in kwargs else sample_task.__name__

    sample_task(tag="tag1")

    task_obj = hence_config.context_search(CTX_FN_BASE, "sample_task")

    assert task_obj["title"] == "sample_task title"
