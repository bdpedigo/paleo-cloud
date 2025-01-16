# %%

import os
from functools import partial
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
from caveclient import CAVEclient
from cloudfiles import CloudFiles
from paleo import (
    apply_edit_sequence,
    check_graph_changes,
    get_initial_graph,
    get_level2_data,
    get_level2_spatial_graphs,
    get_metaedit_counts,
    get_metaedits,
    get_mutable_synapses,
    get_node_aliases,
    get_nucleus_supervoxel,
    get_root_level2_edits,
    map_synapses_to_sequence,
)
from paleo.io import edits_to_json, graph_to_json, json_to_edits, json_to_graph
from taskqueue import TaskQueue, queueable

N_JOBS = int(os.getenv("N_JOBS", -1))
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

client = CAVEclient("minnie65_phase3_v1", version=VERSION)
cf = CloudFiles(BUCKET)
tq = TaskQueue(QUEUE)


def write_df_to_cloud(df, path):
    with BytesIO() as f:
        df.to_csv(f, index=True, compression="gzip")
        cf.put(path, f)
    return 1


def get_path(file, directory):
    current_file = Path(file)
    output_path = (
        current_file.parent.parent / directory / current_file.name.strip(".py")
    )
    output_path.mkdir(exist_ok=True, parents=True)
    return output_path


def bytes_to_df(bytes):
    with BytesIO(bytes) as f:
        df = pd.read_csv(f, index_col=0, compression="gzip")
    return df


# Convert to list of integers
def str_to_list(string_list):
    out = list(map(int, string_list.strip("[]").split()))

    return out


def format_synapses(synapses, side="pre"):
    synapses[f"{side}_pt_position"] = synapses[f"{side}_pt_position"].apply(str_to_list)

    synapses["x"] = synapses[f"{side}_pt_position"].apply(lambda x: x[0])
    synapses["y"] = synapses[f"{side}_pt_position"].apply(lambda x: x[1])
    synapses["z"] = synapses[f"{side}_pt_position"].apply(lambda x: x[2])
    synapses["x"] = synapses["x"] * 4
    synapses["y"] = synapses["y"] * 4
    synapses["z"] = synapses["z"] * 40

    return synapses


def decode_synapses(input, side="pre"):
    out = bytes_to_df(input)
    out = format_synapses(out, side=side)
    return out


def subset_dict(d, keys):
    return {k: d[k] for k in keys}


def extract_edits(root_id):
    if not cf.exists(f"edits/{root_id}_edits.json") or RECOMPUTE:
        network_edits = get_root_level2_edits(root_id, client, n_jobs=N_JOBS)
        out_json = edits_to_json(network_edits)
        cf.put_json(f"edits/{root_id}_edits.json", out_json)

        back_json = cf.get_json(f"edits/{root_id}_edits.json")
        back_edits = json_to_edits(back_json)

        for operation_id, delta in network_edits.items():
            assert delta == back_edits[operation_id]

    try:
        edits = json_to_edits(cf.get_json(f"edits/{root_id}_edits.json"))
    except Exception as e:
        print("Bailing out")
        print(cf.get_json(f"edits/{root_id}_edits.json"))
        print()
        print()
        raise e
    return edits


def extract_graph(root_id):
    if not cf.exists(f"initial_graphs/{root_id}_initial_graph.json") or RECOMPUTE:
        print("Extracting graph for", root_id)
        initial_graph = get_initial_graph(
            root_id, client, verbose=VERBOSE, n_jobs=N_JOBS
        )
        out_json = graph_to_json(initial_graph)
        cf.put_json(
            f"initial_graphs/{root_id}_initial_graph.json", out_json, compress="gzip"
        )

        back_json = cf.get_json(f"initial_graphs/{root_id}_initial_graph.json")
        back_graph = json_to_graph(back_json)

        diffs = np.setdiff1d(
            np.array(back_graph.nodes()), np.array(initial_graph.nodes())
        )
        assert len(diffs) == 0

    initial_graph = json_to_graph(
        cf.get_json(f"initial_graphs/{root_id}_initial_graph.json")
    )
    return initial_graph


def extract_synapses(root_id):
    if not cf.exists(f"post_synapses/{root_id}_post_synapses.csv.gz") or RECOMPUTE:
        print("Extracting synapses for", root_id)
        edits_json = cf.get_json(f"edits/{root_id}_edits.json")
        edits = json_to_edits(edits_json)
        pre_synapses, post_synapses = get_mutable_synapses(
            root_id, edits, client, sides="both", verbose=VERBOSE, n_jobs=N_JOBS
        )
        pre_synapses = pre_synapses.drop(columns=["created", "superceded_id", "valid"])
        post_synapses = post_synapses.drop(
            columns=["created", "superceded_id", "valid"]
        )
        write_df_to_cloud(pre_synapses, f"pre_synapses/{root_id}_pre_synapses.csv.gz")
        write_df_to_cloud(
            post_synapses, f"post_synapses/{root_id}_post_synapses.csv.gz"
        )
    pre_synapses = decode_synapses(
        cf.get(f"pre_synapses/{root_id}_pre_synapses.csv.gz"), side="pre"
    )
    post_synapses = decode_synapses(
        cf.get(f"post_synapses/{root_id}_post_synapses.csv.gz"), side="post"
    )
    return pre_synapses, post_synapses


@queueable
def run_for_root(root_id):
    print("Running for", root_id)
    edits = extract_edits(root_id)
    initial_graph = extract_graph(root_id)
    pre_synapses, post_synapses = extract_synapses(root_id)

    metaedits, edit_map = get_metaedits(edits)
    metaedit_counts = get_metaedit_counts(edit_map)
    nuc_supervoxel_id = get_nucleus_supervoxel(root_id, client)
    anchor_nodes = get_node_aliases(nuc_supervoxel_id, client, stop_layer=2)

    if (
        not cf.exists(f"post_synapse_sequences/{root_id}_idealized_sequence.json")
        or RECOMPUTE
    ):
        for sequence_name, sequence_edits in [
            ("historical", edits),
            ("idealized", metaedits),
        ]:
            graphs_by_state = apply_edit_sequence(
                initial_graph,
                sequence_edits,
                anchor_nodes,
                return_graphs=True,
                include_initial=True,
                remove_unchanged=True,
            )
            level2_data = get_level2_data(graphs_by_state, client)
            spatial_graphs_by_state = get_level2_spatial_graphs(
                graphs_by_state, level2_data=level2_data, client=client
            )
            is_new_level2_state = check_graph_changes(spatial_graphs_by_state)

            # skeletons_by_state, _ = skeletonize_sequence(
            #     graphs_by_state,
            #     root_id=root_id,
            #     client=client,
            #     level2_data=level2_data,
            # )

            used_states = [k for k, v in is_new_level2_state.items() if v]

            for side, synapses in zip(["pre", "post"], [pre_synapses, post_synapses]):
                synapses_by_state = map_synapses_to_sequence(
                    synapses, graphs_by_state, side=side
                )
                synapses_by_state = subset_dict(synapses_by_state, used_states)
                synapse_ids_by_state = {
                    k: list(v.keys()) for k, v in synapses_by_state.items()
                }

                cf.put_json(
                    f"{side}_synapse_sequences/{root_id}_{sequence_name}_sequence.json",
                    synapse_ids_by_state,
                )

            state_info = pd.DataFrame(index=used_states)
            if sequence_name == "idealized":
                state_info["n_edits"] = (
                    state_info.index.map(metaedit_counts).fillna(0).astype(int)
                )
            elif sequence_name == "historical":
                state_info["n_edits"] = np.ones(len(state_info), dtype=int)
            state_info["cumulative_n_edits"] = state_info["n_edits"].cumsum()

            write_df_to_cloud(
                state_info,
                f"state_info/{root_id}_{sequence_name}_state_info.csv.gz",
            )
    print("Done for", root_id)


# %%
def stop_fn(elapsed_time):
    if elapsed_time > 3600 * 2:
        print("Timed out")
        return True


lease_seconds = 1 * 3600
# if RUN:
if True:
    tq.poll(lease_seconds=lease_seconds, verbose=False, tally=False)

# %%


REQUEST = False
if REQUEST:
    proofreading_table = client.materialize.query_table(
        "proofreading_status_and_strategy"
    )
    proofreading_table.query(
        "strategy_axon.isin(['axon_fully_extended', 'axon_partially_extended'])",
        inplace=True,
    )
    root_ids = proofreading_table["pt_root_id"].unique()

    # files = list(cf.list("post_synapses"))
    # processed_roots = [int(f.split("/")[1].split("_")[0]) for f in files]

    # root_ids = np.setdiff1d(root_ids, processed_roots)

    # tasks = [partial(extract_graph, root_id) for root_id in root_ids]
    tasks = [partial(run_for_root, root_id) for root_id in root_ids]
    tq.insert(tasks)

# %%

proofreading_table = client.materialize.query_table("proofreading_status_and_strategy")
proofreading_table.query(
    "strategy_axon.isin(['axon_fully_extended', 'axon_partially_extended'])",
    inplace=True,
)
root_ids = proofreading_table["pt_root_id"].unique()

# %%
root_id = 864691135105609293
run_for_root(root_id)
# %%
out = cf.get_json(f"edits/{root_id}_edits.json")
