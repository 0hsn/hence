# Setup

## Using as a package

- To use Hence as python package just run

    ```shell
    pip install -U git+https://github.com/0hsn/hence.git@main
    ```

    > I intent to keep `main` branch stable. I will also release stable tags. Therefore, anyone should be able to install from a specific tag.

    ```shell
    pip install -U git+https://github.com/0hsn/hence.git@v0.9.0
    ```

## Development

This software use Pipenv for development. Therefore please [install Pipenv](https://pipenv.pypa.io/en/latest/installation.html#installing-pipenv) it beforehand.

- First clone the repository to your local work directory

- To install the development setup

    ```shell
    pipenv install --dev
    ```

- To run a example script

    ```shell
    pipenv run python -m pytest tests/samples/[SCRIPT_NAME].py
    ```

## Testing

This software use pytest for testing.

- Run all test scripts as follows

    ```shell
    pipenv run python -m pytest -s
    ```