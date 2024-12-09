# %%

import os
from functools import partial
from io import BytesIO

import numpy as np
from cloudfiles import CloudFiles
from taskqueue import TaskQueue, queueable

from caveclient import CAVEclient, set_session_defaults
from paleo import get_initial_graph, get_mutable_synapses
from paleo.io import graph_to_json, json_to_edits, json_to_graph

N_JOBS = int(os.getenv("N_JOBS", 1))
VERSION = int(os.getenv("VERSION", 1181))
QUEUE = os.getenv("QUEUE", "ben-paleo")
QUEUE = f"https://sqs.us-west-2.amazonaws.com/629034007606/{QUEUE}"
BUCKET = os.getenv("BUCKET", "allen-minnie-phase3/paleo_edits")
BUCKET = f"gs://{BUCKET}"
BUCKET = BUCKET + "/v" + str(VERSION)
RUN = os.getenv("RUN", "True").lower() == "true"
REQUEST = os.getenv("REQUEST", "False").lower() == "true"
VERBOSE = os.getenv("VERBOSE", "True").lower() == "true"
RECOMPUTE = os.getenv("RECOMPUTE", "False").lower() == "true"
BACKOFF_FACTOR = float(os.getenv("BACKOFF_FACTOR", 0.1))
BACKOFF_MAX = float(os.getenv("BACKOFF_MAX", 120))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))

set_session_defaults(max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR, backoff_max=BACKOFF_MAX)
client = CAVEclient("minnie65_phase3_v1", version=VERSION)
cf = CloudFiles(BUCKET)
tq = TaskQueue(QUEUE)


def write_df_to_cloud(df, path):
    with BytesIO() as f:
        df.to_csv(f, index=True, compression="gzip")
        cf.put(path, f)
    return 1


@queueable
def extract_graph(root_id):
    if cf.exists(f"initial_graphs/{root_id}_initial_graph.json") and not RECOMPUTE:
        return 1
    print("Extracting graph for", root_id)
    initial_graph = get_initial_graph(root_id, client, verbose=VERBOSE, n_jobs=N_JOBS)
    out_json = graph_to_json(initial_graph)
    cf.put_json(
        f"initial_graphs/{root_id}_initial_graph.json", out_json, compress="gzip"
    )

    back_json = cf.get_json(f"initial_graphs/{root_id}_initial_graph.json")
    back_graph = json_to_graph(back_json)

    diffs = np.setdiff1d(np.array(back_graph.nodes()), np.array(initial_graph.nodes()))
    assert len(diffs) == 0
    return 1


@queueable
def extract_synapses(root_id):
    if not cf.exists(f"edits/{root_id}_edits.json"):
        return 0
    if cf.exists(f"post_synapses/{root_id}_post_synapses.csv.gz") and not RECOMPUTE:
        return 1
    print("Extracting synapses for", root_id)
    edits_json = cf.get_json(f"edits/{root_id}_edits.json")
    edits = json_to_edits(edits_json)
    pre_synapses, post_synapses = get_mutable_synapses(
        root_id, edits, client, sides="both", verbose=VERBOSE, n_jobs=N_JOBS
    )
    pre_synapses = pre_synapses.drop(columns=["created", "superceded_id", "valid"])
    post_synapses = post_synapses.drop(columns=["created", "superceded_id", "valid"])
    write_df_to_cloud(pre_synapses, f"pre_synapses/{root_id}_pre_synapses.csv.gz")
    write_df_to_cloud(post_synapses, f"post_synapses/{root_id}_post_synapses.csv.gz")
    return 1


# %%
def stop_fn(elapsed_time):
    if elapsed_time > 3600 * 2:
        print("Timed out")
        return True


lease_seconds = 1 * 3600
# if RUN:
if False:
    tq.poll(lease_seconds=lease_seconds, verbose=False, tally=False)

# %%
REQUEST = True
if REQUEST:
    proofreading_table = client.materialize.query_table(
        "proofreading_status_public_release"
    )
    root_ids = proofreading_table["pt_root_id"].unique()

    files = list(cf.list('post_synapses'))
    processed_roots = [int(f.split('/')[1].split("_")[0]) for f in files]

    root_ids = np.setdiff1d(root_ids, processed_roots)

    # tasks = [partial(extract_graph, root_id) for root_id in root_ids]
    tasks = [partial(extract_synapses, root_id) for root_id in root_ids]
    tq.insert(tasks)

