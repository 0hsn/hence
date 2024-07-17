"""
Tests for workgroup
"""

import pytest

from hence import AbstractWork, WorkGroup, WorkExecFrame, work


class TestWorkGroup:
    """TestTask"""

    @staticmethod
    def test__create_wg__pass_for_list_of_work_exec_frames():
        """test  create wg  pass for list of work exec frames"""

        class ImplementedWork(AbstractWork):
            """ImplementedWork"""

            def __work__(self, **kwargs):
                print(type(self).__name__)

        wg = WorkGroup(
            [
                WorkExecFrame(function=ImplementedWork()),
                WorkExecFrame(function=ImplementedWork()),
                WorkExecFrame(function=ImplementedWork()),
            ]
        )

        assert isinstance(wg, WorkGroup)
        assert len(wg.vertices) == 3

        for item in wg.vertices:
            assert isinstance(item, WorkExecFrame)

    @staticmethod
    def test__create_wg__pass_for_empty_list_of_work_exec_frames():
        """test  create wg  pass for empty list of work exec frames"""

        wg = WorkGroup([])

        assert isinstance(wg, WorkGroup)
        assert len(wg.vertices) == 0

    @staticmethod
    def test__create_wg__fail_when_wrong_step_set():
        """test create work group fail when wrong step set"""

        class ImplementedWork(AbstractWork):
            """ImplementedWork"""

            def __work__(self, **kwargs):
                print(type(self).__name__)

        with pytest.raises(TypeError):
            WorkGroup([ImplementedWork()])

    @staticmethod
    def test__create_wg__pass_with_work_list_params(capsys):
        """test create work group fail when wrong step set"""

        class ImplementedWork(AbstractWork):
            """ImplementedWork"""

            def __work__(self, **kwargs):
                print(type(self).__name__)
                print(";".join([f"{key}={val}" for key, val in kwargs.items()]))

        wg = WorkGroup(
            [
                WorkExecFrame(
                    title="ImplementedWork",
                    function=ImplementedWork(),
                    function_params={"param1": 1, "param2": "ab"},
                ),
                WorkExecFrame(
                    title="ImplementedWork",
                    function=ImplementedWork(),
                    function_params={"param1": 2, "param2": "bc"},
                ),
            ]
        )

        wg.execute_dag()

        out, _ = capsys.readouterr()

        assert (
            out
            == "ImplementedWork\nparam1=1;param2=ab;__works__={};__before__=Ellipsis\nImplementedWork\nparam1=2;param2=bc;__works__={};__before__=Ellipsis\n"
        )

    @staticmethod
    def test__create_wg__pass_for_dag_creation():
        """test create work group pass for dag creation"""

        class ImplementedWork1(AbstractWork):
            """ImplementedWork1"""

            def __work__(self, **kwargs):
                print(type(self).__name__)

        class ImplementedWork2(AbstractWork):
            """ImplementedWork2"""

            def __work__(self, **kwargs):
                print(type(self).__name__)

        class ImplementedWork3(AbstractWork):
            """ImplementedWork3"""

            def __work__(self, **kwargs):
                print(type(self).__name__)

        wg = WorkGroup([])

        wg.append(WorkExecFrame(function=ImplementedWork1()))
        wg.append(WorkExecFrame(function=ImplementedWork2()))
        wg.append(WorkExecFrame(function=ImplementedWork3()))

        wg.setup_dag()

        assert wg._dag.vertex_size() == 3
        assert wg._dag.edge_size() == 2


class TestWorkGroupExecute:
    """TestWorkGroupExecute"""

    @staticmethod
    def test__execute_wg__pass_when_steps_are_right(capsys):
        """test create_task pass for dag creation"""

        class ImplementedWork1(AbstractWork):
            """ImplementedWork1"""

            def __work__(self, **kwargs):
                print(type(self).__name__)

        class ImplementedWork2(AbstractWork):
            """ImplementedWork2"""

            def __work__(self, **kwargs):
                print(type(self).__name__)

        class ImplementedWork3(AbstractWork):
            """ImplementedWork3"""

            def __work__(self, **kwargs):
                print(type(self).__name__)

        wg = WorkGroup(
            [
                WorkExecFrame(function=ImplementedWork1()),
                WorkExecFrame(function=ImplementedWork2()),
                WorkExecFrame(function=ImplementedWork3()),
            ]
        )

        wg.setup_dag()

        resp = wg.execute_dag()
        out, _ = capsys.readouterr()

        assert out == "ImplementedWork1\nImplementedWork2\nImplementedWork3\n"
        assert len(resp) == 3

    @staticmethod
    def test__execute_wg__pass_get_other_state_results(capsys):
        """test__execute_wg__pass_get_other_state_results"""

        class ImplementedWork1(AbstractWork):
            """ImplementedWork1"""

            def __work__(self, **kwargs):
                print("ImplementedWork1 kwargs:", kwargs)
                return type(self).__name__

        class ImplementedWork2(AbstractWork):
            """ImplementedWork2"""

            def __work__(self, **kwargs):
                print("ImplementedWork2 kwargs:", kwargs)
                return type(self).__name__

        class ImplementedWork3(AbstractWork):
            """ImplementedWork3"""

            def __work__(self, **kwargs):
                print("ImplementedWork3 kwargs:", kwargs)
                return type(self).__name__

        wg = WorkGroup(
            [
                WorkExecFrame(id_="Work1", function=ImplementedWork1()),
                WorkExecFrame(id_="Work2", function=ImplementedWork2()),
                WorkExecFrame(id_="Work3", function=ImplementedWork3()),
            ]
        )

        wg.setup_dag()

        resp = wg.execute_dag()

        out, _ = capsys.readouterr()

        assert out == (
            """ImplementedWork1 kwargs: {'__works__': {}, '__before__': Ellipsis}\nImplementedWork2 kwargs: {'__works__': {'Work1': 'ImplementedWork1'}, '__before__': Ellipsis}\nImplementedWork3 kwargs: {'__works__': {'Work1': 'ImplementedWork1', 'Work2': 'ImplementedWork2'}, '__before__': Ellipsis}\n"""
        )
        assert len(resp) == 3

        for wef in resp:
            assert wef.function_out != ""

    @staticmethod
    def test__execute_wg__pass_callable_params_get_other_state_results(capsys):
        """test__execute_wg__pass_function_params_get_other_state_results"""

        class ImplementedWork1(AbstractWork):
            """ImplementedWork1"""

            def __work__(self, **kwargs):
                print("ImplementedWork1 kwargs:", kwargs)
                return type(self).__name__

        class ImplementedWork2(AbstractWork):
            """ImplementedWork2"""

            def __work__(self, **kwargs):
                print("ImplementedWork2 kwargs:", kwargs)
                return type(self).__name__

        class ImplementedWork3(AbstractWork):
            """ImplementedWork3"""

            def __work__(self, **kwargs):
                print("ImplementedWork3 kwargs:", kwargs)
                return type(self).__name__

        wg = WorkGroup([])

        wg.append(
            WorkExecFrame(
                id_="Work1",
                function=ImplementedWork1(),
                function_params={"as": 2, "of": "date0"},
            )
        )
        wg.append(
            WorkExecFrame(
                id_="Work2",
                function=ImplementedWork2(),
                function_params={"as": 2, "of": "date1"},
            )
        )
        wg.append(
            WorkExecFrame(
                id_="Work3",
                function=ImplementedWork3(),
                function_params={"as": 2, "of": "date2"},
            )
        )

        wg.setup_dag()

        resp = wg.execute_dag()

        out, _ = capsys.readouterr()

        assert out == (
            """ImplementedWork1 kwargs: {'as': 2, 'of': 'date0', '__works__': {}, '__before__': Ellipsis}\nImplementedWork2 kwargs: {'as': 2, 'of': 'date1', '__works__': {'Work1': 'ImplementedWork1'}, '__before__': Ellipsis}\nImplementedWork3 kwargs: {'as': 2, 'of': 'date2', '__works__': {'Work1': 'ImplementedWork1', 'Work2': 'ImplementedWork2'}, '__before__': Ellipsis}\n"""
        )
        assert len(resp) == 3

        for wef in resp:
            assert wef.function_out != ""

    @staticmethod
    def test__execute_wg__pass_function_params_get_other_state_results(capsys):
        """test__execute_wg__pass_function_params_get_other_state_results"""

        @work()
        def implemented_work1(**kwargs):
            """implemented_work1"""

            print("implemented_work1 kwargs:", kwargs)
            return implemented_work1.__name__

        @work()
        def implemented_work2(**kwargs):
            """implemented_work2"""

            print("implemented_work2 kwargs:", kwargs)
            return implemented_work2.__name__

        @work()
        def implemented_work3(**kwargs):
            """implemented_work3"""

            print("implemented_work3 kwargs:", kwargs)
            return implemented_work3.__name__

        wg = WorkGroup([])

        wg.append(
            WorkExecFrame(
                id_="Work1",
                function=implemented_work1,
                function_params={"as": 2, "of": "date0"},
            )
        )
        wg.append(
            WorkExecFrame(
                id_="Work2",
                function=implemented_work2,
                function_params={"as": 2, "of": "date1"},
            )
        )
        wg.append(
            WorkExecFrame(
                id_="Work3",
                function=implemented_work3,
                function_params={"as": 2, "of": "date2"},
            )
        )

        wg.setup_dag()

        resp = wg.execute_dag()

        out, _ = capsys.readouterr()

        assert out == (
            """implemented_work1 kwargs: {'as': 2, 'of': 'date0', '__works__': {}, '__before__': Ellipsis}\nimplemented_work2 kwargs: {'as': 2, 'of': 'date1', '__works__': {'Work1': 'implemented_work1'}, '__before__': Ellipsis}\nimplemented_work3 kwargs: {'as': 2, 'of': 'date2', '__works__': {'Work1': 'implemented_work1', 'Work2': 'implemented_work2'}, '__before__': Ellipsis}\n"""
        )
        assert len(resp) == 3

        for wef in resp:
            assert wef.function_out != ""


class TestWorkGroupListCapability:
    """TestWorkGroupListCapability"""

    @staticmethod
    def test___validate_type__pass():
        """test__from_works__pass"""

        class ImplementedWork(AbstractWork):
            """ImplementedWork"""

            def __work__(self, **kwargs):
                print(type(self).__name__)

        wg = WorkGroup([])

        wg.append(WorkExecFrame(function=ImplementedWork()))
        wg.append(WorkExecFrame(function=ImplementedWork()))
        wg.append(WorkExecFrame(function=ImplementedWork()))

        assert isinstance(wg, WorkGroup)
        assert all(isinstance(item, WorkExecFrame) for item in wg._works)

    @staticmethod
    def test___validate_type__fail_for_abstract_work_missing_kwargs():
        """test___validate_type__fail_for_abstract_work_missing_kwargs"""

        class ImplementedWork(AbstractWork):
            """ImplementedWork"""

            def __work__(self):
                print(type(self).__name__)

        wg = WorkGroup([])

        with pytest.raises(TypeError):
            wg.append(WorkExecFrame(function=ImplementedWork()))

    @staticmethod
    def test___validate_type__fails_for_callable():
        """test__from_works__fails_for_callable"""

        with pytest.raises(TypeError):
            WorkGroup(
                [
                    map,
                    map,
                    map,
                ]
            )

    @staticmethod
    def test___validate_type__passes_for_work_callable(capsys):
        """test__from_work_dec__passes_for_work_callable"""

        @work()
        def print_s(string, **kwargs):
            print(string)

        wg = WorkGroup([])
        wg.append(
            WorkExecFrame(
                function=print_s, function_params={"string": print_s.__name__}
            )
        )

        wg.setup_dag()
        wg.execute_dag()
        out, _ = capsys.readouterr()

        assert out == "print_s\n"
