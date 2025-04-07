"""
# A very minimal web scraping examples using class
#
# ----------------------------------------
# Following code example SHOULD NOT be used
# in production scenario
# ----------------------------------------
"""

import csv
from urllib import request

from hence import Pipeline, PipelineContext

pipeline = Pipeline()


@pipeline.add_task()
def fetch_content():
    """Fetch the content of example.org"""

    with request.urlopen("https://example.org/") as response:
        return response.read().decode('utf-8')


@pipeline.add_task(pass_ctx=True)
def get_the_title(ctx: PipelineContext) -> dict:
    """Parse the content in <title>"""  

    if (html := ctx.result["fetch_content"]) is not None:
        html.find("<h1>")
        title = html[html.find("<h1>") + len("<h1>") : html.find("</h1>")]
        body = html[html.find("<p>") + len("<p>") : html.find("</p>")]

        return dict(title=title, body=body)

    return {}


@pipeline.add_task(pass_ctx=True)
def save_to_csv(ctx: PipelineContext) -> None:
    """save the content to csv"""

    if (ret := ctx.result["get_the_title"]) is not None:
        with open("example.org-data.csv", "w+", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)

            writer.writerow(["title", "description"])
            writer.writerow([ret["title"], ret["body"]])


pipeline.run()
