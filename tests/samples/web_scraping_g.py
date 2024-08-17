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

from hence import Utils, group, run_group, task

mws = group("web_scraping")


@mws
@task(title="Get the content")
def fetch_content(**kwargs):
    """Fetch the content of example.org"""

    with request.urlopen("https://example.org/") as response:
        return response.read().hex()


@mws
@task(title="Parse the title of the content")
def get_the_title(**kwargs) -> dict:
    """Parse the content in <title>"""

    if (html := Utils.get_step(0, "web_scraping").result) is not None:
        html = bytes.fromhex(html).decode("utf-8")

        html.find("<h1>")
        title = html[html.find("<h1>") + len("<h1>") : html.find("</h1>")]
        body = html[html.find("<p>") + len("<p>") : html.find("</p>")]

        return dict(title=title, body=body)

    return {}


@mws
@task(title="Save the content to csv")
def save_to_csv(**kwargs) -> None:
    """save the content to csv"""

    if (ret := Utils.get_step(1, "web_scraping").result) is not None:
        with open("example.org-data.csv", "w+", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)

            writer.writerow(["title", "description"])
            writer.writerow([ret["title"], ret["body"]])


Utils.enable_logging(True)

run_group("web_scraping", [])
