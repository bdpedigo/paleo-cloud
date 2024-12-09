# %%

import os
from functools import partial

from cloudfiles import CloudFiles
from taskqueue import TaskQueue, queueable

from caveclient import CAVEclient, set_session_defaults
from paleo import get_root_level2_edits
from paleo.io import edits_to_json, json_to_edits

N_JOBS = int(os.getenv("N_JOBS", 1))
VERSION = int(os.getenv("VERSION", 1181))
QUEUE = os.getenv("QUEUE", "ben-paleo")
QUEUE = f"https://sqs.us-west-2.amazonaws.com/629034007606/{QUEUE}"
BUCKET = os.getenv("BUCKET", "allen-minnie-phase3/paleo_edits")
BUCKET = f"gs://{BUCKET}"
BUCKET = BUCKET + "/v" + str(VERSION)
RUN = os.getenv("RUN", "True").lower() == "true"
REQUEST = os.getenv("REQUEST", "False").lower() == "true"


set_session_defaults(max_retries=5, backoff_factor=0.9, backoff_max=240)
client = CAVEclient("minnie65_phase3_v1", version=VERSION)
cf = CloudFiles(BUCKET)
tq = TaskQueue(QUEUE)


@queueable
def extract_edits(root_id):
    network_edits = get_root_level2_edits(root_id, client, n_jobs=N_JOBS)

    out_json = edits_to_json(network_edits)
    cf.put_json(f"{root_id}_edits.json", out_json)

    back_json = cf.get_json(f"{root_id}_edits.json")
    back_edits = json_to_edits(back_json)

    for operation_id, delta in network_edits.items():
        assert delta == back_edits[operation_id]

    return 1


def stop_fn(elapsed_time):
    if elapsed_time > 3600 * 6:
        print("Timed out")
        return True


lease_seconds = 6 * 3600
if False:
    tq.poll(lease_seconds=lease_seconds, verbose=False, tally=False)

# %%
if True:
    proofreading_table = client.materialize.query_table(
        "proofreading_status_public_release"
    )
    root_ids = proofreading_table["pt_root_id"].unique()

    tasks = [partial(extract_edits, root_id) for root_id in root_ids]

    # tq.insert(tasks)
