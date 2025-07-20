import os
import time
import sys
import re
import networkx as nx
import json
from datetime import datetime, timezone
import subprocess
import pprint
import csv

# Parse stdout
def parse_stdout(stdout, workflow_meta):
    lines = stdout.split('\n')
    for line in lines:
        #print(f"{line=}")
        if "version" in line:
            x = line.split("version")[1].strip()
            print(f"Nextflow version:\n\t{x}")

        elif "Launching" in line:
            x = line.split("`")[1].strip()
            workflow_meta["repo"] = x
            print(f"Workflow repo:\n\t{x}")

        elif "runName" in line:
            x = line[line.find(":")+1:].strip()
            workflow_meta["runName"] = x
            print(f"runName:\n\t{x}")

        elif "Completed at" in line:
            x = line[line.find(":")+1:].strip() + " -1000"      #HST is -10 hours to UTC
            y = datetime.strptime(x, "%d-%b-%Y %H:%M:%S %z")

            #dunno if %d (zero-padded day of the month) or (%-d not zero-padded)
            #of if %H (zero-padded 24 hour clock hour) or (%-H not zero-padded)
            #%S (seconds) or (%-S)
            #print("executedAt:\n\t{}".format(x))
            #print("converted:\n\t{}".format(y.isoformat()))

            print(f"executedAt:\n\t{y.isoformat()}")
            workflow_meta["executedAt"] = str(y.isoformat())

        elif "Duration" in line:
            x = line[line.find(":")+1:].strip()
            print("makespan:\n\t{}".format(x))

            seconds = 0
            temp    = x
            if "h" in line:
                seconds += int(temp.split("h")[0].strip()) * 60 * 60
                temp     = temp.split("h")[1].strip()
            if "m" in line:
                seconds += int(temp.split("m")[0].strip()) * 60
                temp     = temp.split("m")[1].strip()
            if "s" in line:
                seconds += int(temp.split("s")[0].strip())
            
            print(f"makespan in seconds:\n\t{seconds}")

            workflow_meta["makespanInSeconds"] = seconds
            
    # pprint.pprint(workflow_meta)
    return workflow_meta

# Parse scripts log
def parse_scripts(log, scripts):
    lines = log.split("END_PROCESS_SCRIPT")
    for i in range(len(lines)-1):       #last line is blank
        x = lines[i].split("START_PROCESS_SCRIPT")
        scripts[x[0].strip()] = x[1].strip()
        #print(f"{x[0].strip()} {x[1].strip()}")

#Parse filepath_trace
#Assumes the following trace fields
#0,       1,       2,        3,    4,   5,     6,     7,          8,           9
#task_id, process, realtime, %cpu, rss, rchar, wchar, read_bytes, write_bytes, workdir
def parse_trace(filepath_trace, task_id, processes, realtime, pct_cpu, rss, rchar, wchar, read_bytes, write_bytes, work_dir):
    def parse_field(curr_id, f_dict,  f_str):
        if "-" == f_str:
            f_dict[curr_id] = 0.
        else:
            f_dict[curr_id] = float(f_str)

    with open(filepath_trace, "r") as fp:
        for count, line in enumerate(fp):
            if count == 0:      #skip first line
                continue

            fields = line.split("\t")
            #print("Line {}: {}".format(count, fields))

            curr_id = fields[0].strip()
            task_id.append(curr_id)

            processes[curr_id] = fields[1].strip()

            parse_field(curr_id, realtime, fields[2].strip())
            parse_field(curr_id, pct_cpu,  fields[3].strip())
            parse_field(curr_id, rss,      fields[4].strip())

            parse_field(curr_id, rchar,       fields[5].strip())
            parse_field(curr_id, wchar,       fields[6].strip())
            parse_field(curr_id, read_bytes,  fields[7].strip())
            parse_field(curr_id, write_bytes, fields[8].strip())


            # realtime [curr_id] = fields[2].strip()
            # pct_cpu  [curr_id] = fields[3].strip()
            # rss      [curr_id] = fields[4].strip()

            # rchar      [curr_id] = fields[5].strip()
            # wchar      [curr_id] = fields[6].strip()
            # read_bytes [curr_id] = fields[7].strip()
            # write_bytes[curr_id] = fields[8].strip()

            work_dir[curr_id] = fields[9].strip()

    return processes


#Parse filepath_dag file
def parse_dag(filepath_dag, parents, children):
    G = nx.nx_agraph.read_dot(filepath_dag)

    def check_graph(G):     #checks if the graph contains a vertex that is not a process (i.e., does not have a label)
        for node in G:
            if "label" not in G.nodes[node]:
                return 1
        return 0

    check_graph(G)

    while (check_graph(G) == 1):
        for node in G:
            if "label" not in G.nodes[node]:
                in_nodes  = G.pred[node].keys()
                out_nodes = G.succ[node].keys()
                G.remove_node(node)

                for in_node in in_nodes:
                    for out_node in out_nodes:
                        #print("\t({}, {})".format(in_node, out_node))
                        G.add_edge(in_node, out_node)

                break

    for node in G:
        #print("\nnode {}: {}".format(node, G.nodes[node]))
        process = G.nodes[node]["label"]

        parents [process] = []
        children[process] = []

        if G.in_degree[node] > 0:
            # print("\tpred: {}".format(list(G.pred[node].keys())))
            for i, pred in enumerate(G.pred[node].keys()):
                #print("pred {} = {}".format(pred, G.nodes[pred]["label"]))
                parents[process].append(G.nodes[pred]["label"])

        if G.out_degree[node] > 0:
            # print("\tsucc: {}".format(list(G.succ[node].keys())))
            for i, succ in enumerate(G.succ[node].keys()):
                #print("succ {} = {}".format(succ, G.nodes[succ]["label"]))
                children[process].append(G.nodes[succ]["label"])

# Parse input files of processes
def parse_inputs(csv_file="input.csv"):
    process_inputs = {}

    with open(csv_file, newline ='') as f:
        reader = csv.DictReader(f, delimiter= ';')
        for row in reader:
            process = row['name'].replace(':', '.').split()[0]
            file_path = row['path']
            size = int(row['size'])
            if process not in process_inputs:
                process_inputs[process] = []
            process_inputs[process].append({
                'id': file_path,
                'sizeInBytes': size if size else 0,
            })

    return process_inputs

def parse_outputs(csv_file="output.csv"):
    process_outputs = {}

    with open(csv_file, newline ='') as f:
        reader = csv.DictReader(f, delimiter= ';')
        for row in reader:
            process = row['name'].replace(':', '.').split()[0]
            file_path = row['path']
            size = int(row['size'])
            if process not in process_outputs:
                process_outputs[process] = []
            process_outputs[process].append({
                'id': file_path,
                'sizeInBytes': size 
            })

    return process_outputs

def buildAndWriteJSONSchema(input_dict, output_dict, processes, task_id, realtime, pct_cpu, rss, rchar, wchar, workflow_meta):
    #7. Create list of tasks for WfCommons JSON output
    #i. Replace : with . in parent/children elements
    rep_parents  = {}
    rep_children = {}
    for i in task_id:
        rep_parents [processes[i]] = []
        rep_children[processes[i]] = []

        for j in parents[processes[i]]:
            rep_parents[processes[i]].append(j.replace(':', '.'))

        for j in children[processes[i]]:
            rep_children[processes[i]].append(j.replace(':', '.'))

    # Create tasks
    tasks = []
    processes2 = []
    for i in task_id:
        curr_task                     = {}
        curr_task["name"]             = processes[i].replace(':', '.')
        # curr_task["id"]               = i
        curr_task["id"]               = processes[i].replace(':', '.')
        curr_task["type"]             = "compute"
        # command = {"program": scripts[i], "arguments": []}
        # curr_task["command"]          = command
        curr_process = processes[i].replace(':', '.')
        if curr_process in input_dict:
            curr_task["inputFiles"] = [f["id"] for f in input_dict[curr_process]]
        if curr_process in output_dict:
            curr_task["outputFiles"] = [f["id"] for f in output_dict[curr_process]]

        # No longer track parents/children (see README)
        curr_task["parents"]          = rep_parents[processes[i]]
        curr_task["children"]         = rep_children[processes[i]]
        # curr_task["parents"]          = []
        # curr_task["children"]         = []
        processes2.append(curr_process)

        tasks.append(curr_task)

    execution_tasks = []
    for t in task_id:
        exec_task = {
            "id": processes[t].replace(':', '.'),
            "runtimeInSeconds": float(realtime[t])/1000.0,
            "avgCPU": pct_cpu[t],
            "memoryInBytes": rss[t],
            "bytesRead": int(rchar[t]),
            "bytesWritten": int(wchar[t]), 
        }
        execution_tasks.append(exec_task)

    files_array = []
    all_files = {**input_dict, **output_dict}

    for processes, files in all_files.items():
        for file_dict in files:
            file_id = file_dict.get("id", file_dict.get("id"))
            # if file_id not in seen:
            files_array.append({
            "id": file_id,
            "sizeInBytes": file_dict["sizeInBytes"],
        })


    # Create the specification
    specification = {}
    specification["tasks"] = tasks
    specification["files"] = files_array

    workflow = {}
    workflow["specification"] = specification
    # workflow["tasks"] = tasks

    # Get machine info
    workflow["machines"] = []
    single_machine = {}

    completed_process = subprocess.run(["uname", "-n"], capture_output=True, encoding="utf-8")
    single_machine["nodeName"] = str(completed_process.stdout).strip()

    completed_process = subprocess.run(["uname", "-s"], capture_output=True, encoding="utf-8")
    single_machine["system"] = str(completed_process.stdout).strip().lower()

    completed_process = subprocess.run(["uname", "-r"], capture_output=True, encoding="utf-8")
    single_machine["release"] = str(completed_process.stdout).strip()

    completed_process = subprocess.run(["uname", "-m"], capture_output=True, encoding="utf-8")
    single_machine["architecture"] = str(completed_process.stdout).strip()

    cpu = {}
    core_command = "lscpu | awk '/^CPU\\(s\\):/ {print $2}'"
    core_count = subprocess.check_output(core_command, shell=True).decode().strip()
    cpu["coreCount"] = int(core_count)

    # Get clock rate in Hz
    # This does not work on all systems, for me it outputted only Processor 0.
    # command = "cat /proc/cpuinfo | grep 'model name' | sed 's/.* //' | sed 's/G.*//' | sed 's/\.//' | sed 's/$/0/'"
    # output = output.decode('utf-8').strip().split('\n')[0]  # Convert bytes to string and remove trailing newline
    # cpu["speed"]=int(output)

    # Modified to work with my server.
    command = "lscpu | grep 'CPU max MHz' | awk '{print int($4)}'"
    output = subprocess.check_output(command, shell=True).decode().strip()
    # cpu["speedInMHz"] = int(output) * 1_000_000  # Hz
    cpu["speedInMHz"] = int(output) # MHz

    mem_command = "free | awk '/Mem:/ {print $7}'"
    output = subprocess.check_output(mem_command, shell=True).decode().strip()

    single_machine["cpu"]=cpu
    single_machine["memoryInBytes"]=int(output)
    workflow["machines"].append(single_machine)


    wfcommons                  = {}
    wfcommons["name"]          = workflow_name
    wfcommons["description"]   = "Trace generated from Nextflow (via https://github.com/wfcommons/nextflow_workflow_tracer)"
    wfcommons["createdAt"]     = str(datetime.now(tz=timezone.utc).isoformat())
    wfcommons["schemaVersion"] = "1.5"

    wms         = {}
    wms["name"] = "Nextflow"
    wms["version"] = "25.06.0"
    wms["url"]  = "https://www.nextflow.io/"

    wfcommons["wms"]      = wms
    wfcommons["workflow"] = workflow

    execution = {
        "makespanInSeconds": workflow_meta.get("makespanInSeconds", 0),
        "executedAt": workflow_meta.get("executedAt", ""),
        "tasks": execution_tasks,
        "machines": workflow["machines"]
    }

    node_name = workflow["machines"][0]["nodeName"]
    for exec_task in execution_tasks:
        exec_task["machines"] = node_name

    workflow["execution"] = execution

    with open(outfile, "w") as fp:
        fp.write(json.dumps(wfcommons, indent=4))
    
    
if __name__ == "__main__":
    
    # nextflow_path = "./nextflow-22.10.7/launch.sh"
    # nextflow_path = "/storage/nf-core/exec/nextflow-22.10.7/launch.sh"
    # nextflow_path = "/storage/nf-core/exec/nextflow/build/releases/nextflow-25.06.0-edge-dist"
    nextflow_path = "/home/niklas/.local/bin/nextflow"

    #1. Command line arguments
    argc = len(sys.argv)
    if (argc != 4):
        print(f"Usage: python3 {sys.argv[0]} <workflow name> <work directory> <output file name>")
        quit()

    workflow_name = str(sys.argv[1]) #"hlatyping" #str(sys.argv[1])
    workflow_output = str(sys.argv[2]) #"./output/hlatyping" #str(sys.argv[2])
    outfile = str(sys.argv[3]) #"new_hlatyping.json" #str(sys.argv[3])

    filepath_trace = "trace.txt"        #must match the trace file in trace_nextflow.config
    filepath_dag   = "dag.dot"          #must match the log file in trace_nextflow.config
    filepath_log   = "log.txt"

    if not os.path.exists(nextflow_path):
        print(f"{nextflow_path=} does not exist")
        exit()

    if not os.path.exists(workflow_output):
        print(f"{workflow_output=} does not exist")
        exit()

    #2. Run Nextflow workflow
    print(f"Running /nf-core/{workflow_name}")
    completed_run = subprocess.run([str(os.path.abspath(nextflow_path)), "-log", filepath_log, "run", "nf-core/" + workflow_name, "-profile", "test,docker", "-c", "trace_nextflow.config", "--outdir", workflow_output], capture_output=True, encoding="utf-8")
    print(f"{completed_run.args} : {completed_run.stdout}")
    
    # Writing to stdout
    with open("stdout.txt", "w") as file:
        file.write(completed_run.stdout)

    #3. Parse filepath_trace
    task_id     = []
    processes   = {}
    realtime    = {}
    pct_cpu     = {}
    rss         = {}
    rchar       = {}
    wchar       = {}
    read_bytes  = {}
    write_bytes = {}
    work_dir    = {}
    parse_trace(filepath_trace, task_id, processes, realtime, pct_cpu, rss, rchar, wchar, read_bytes, write_bytes, work_dir)

    #4. Parse filepath_dag
    parents  = {}
    children = {}
    parse_dag(filepath_dag, parents, children)

    print("Parsing input and output files...")
    input_files  = parse_inputs()
    output_files = parse_outputs()

    # pprint.pprint(input_files.keys())
    # pprint.pprint(output_files)

    
    #5. Get workflow metadata from stdout
    workflow_meta = {}
    workflow_meta = parse_stdout(completed_run.stdout, workflow_meta)

    #6. Create WfFormat
    buildAndWriteJSONSchema(input_files, output_files, processes, task_id, realtime, pct_cpu, rss, rchar, wchar, workflow_meta)

    


