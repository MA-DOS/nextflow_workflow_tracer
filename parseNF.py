import os
import re
import networkx as nx
import json
from datetime import datetime

workflow_name = 'hlatyping'

filepath_log    = 'log.txt'
filepath_trace  = 'trace.txt'
filepath_script = 'script.txt'
filepath_dag    = 'dag.dot'

print("Workflow:\n\t{}\nLogs:\n\tlog:\t{}\n\ttrace:\t{}\n\tscript:\t{}\n\tdag:\t{}".format(workflow_name, filepath_log, filepath_trace, filepath_script, filepath_dag))

if not os.path.isfile(filepath_log):
    print("ERROR: file path {} does not exist!".format(filepath_log))
if not os.path.isfile(filepath_trace):
    print("ERROR: file path {} does not exist!".format(filepath_trace))
if not os.path.isfile(filepath_script):
    print("ERROR: file path {} does not exist!".format(filepath_script))
if not os.path.isfile(filepath_dag):
    print("ERROR: file path {} does not exist!".format(filepath_dag))

#1. Parse filepath_trace
#Assumes the following trace fields
#0,       1,       2,        3,    4,   5,     6,     7,          8,           9
#process, task_id, realtime, %cpu, rss, rchar, wchar, read_bytes, write_bytes, workdir
processes = []

task_id = {}
realtime = {}
pct_cpu = {}
rss = {}

rchar = {}
wchar = {}
read_bytes = {}
write_bytes = {}

work_dir = {}

with open(filepath_trace, "r") as fp:
    for count, line in enumerate(fp):
        if count == 0:      #skip first line
            continue

        fields = line.split("\t")
        #print("Line {}: {}".format(count, fields))

        process = fields[0].strip()
        processes.append(process)
        
        task_id [process] = fields[1].strip()
        realtime[process] = fields[2].strip()
        pct_cpu [process] = fields[3].strip()
        rss     [process] = fields[4].strip()

        rchar      [process] = fields[5].strip()
        wchar      [process] = fields[6].strip()
        read_bytes [process] = fields[7].strip()
        write_bytes[process] = fields[8].strip()

        work_dir[process] = fields[9].strip()

#2. Parse filepath_script
script = {}
with open(filepath_script, "r") as fp:
    file_string = ""
    for count, line in enumerate(fp):
        file_string += line

    x = file_string.split("END_PROCESS_SCRIPT")

    for i in range(len(x)-1):       #last line in x is blank
        y = x[i].split("START_PROCESS_SCRIPT")

        script[y[0].strip()] = y[1].strip()

#3. Parse filepath_log
wfcommons                  = {}
wfcommons["name"]          = workflow_name
wfcommons["description"]   = "Trace generated from Nextflow (via https://github.com/wfcommons/nextflow_workflow_tracer)"
wfcommons["createdAt"]     = str(datetime.now().isoformat())
wfcommons["schemaVersion"] = "1.3"

wms         = {}
wms["name"] = "Nextflow"
wms["url"]  = "https://www.nextflow.io/"

workflow = {}
workflow["executedAt"] = "TODO"
workflow["makespan"] = "TODO"

with open(filepath_log, "r") as fp:
    found = 0
    for count, line in enumerate(fp):
        if "nextflow.cli.CmdRun" in line:
            #print("Line {}:\n{}".format(count, line))

            if "version" in line:
                x = line.split("version")[1].strip()
                #print("Nextflow version:\n\t{}".format(x))
                wms["version"] = x

                found = found + 1
                if found == 2:
                    break

            elif "Launching" in line:
                x = line.split("`")[1].strip()
                #print("Workflow repo:\n\t{}".format(x))
                wfcommons["repo"] = x

                found = found + 1
                if found == 2:
                    break

wfcommons["wms"]      = wms
wfcommons["workflow"] = workflow
wfcommons["machines"] = "TODO"

files = {}
for i, process in enumerate(processes):
    files[process] = []
    with open(filepath_log, "r") as fp:
        for count, line in enumerate(fp):
            if process in line:
                if "Message arrived" in line:
                    #print("Line {}:\n{}".format(count, line))
                    x = ((line.split("--"))[1].split("=>"))[1].split(",")
                    
                    for item in x:
                        temp = item.strip().replace("[", "").replace("]", "")
                        if "." in temp or "/" in temp:
                            curr_file = {}
                            curr_file["link"] = "input"
                            curr_file["name"] = temp
                            curr_file["size"] = "TODO"
                            files[process].append(curr_file)

                elif "Binding out param:" in line:
                    #print("Line {}:\n{}".format(count, line))
                    x = ((line.split("Binding out param:"))[1].split("="))[1].split(",")

                    for item in x:
                        temp = item.strip().replace("[", "").replace("]", "")
                        if "." in temp or "/" in temp:
                            curr_file = {}
                            curr_file["link"] = "output"
                            curr_file["name"] = temp
                            curr_file["size"] = "TODO"
                            files[process].append(curr_file)

#4. Parse filepath_dag file
G = nx.nx_agraph.read_dot(filepath_dag)

def check_graph(G):
    for node in G:
        if "label" not in G.nodes[node]:
            return 1

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

parents  = {}
children = {}
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

#5. Create list of tasks for wfcommons json output
tasks = []
for i, process in enumerate(processes):
    curr_task                 = {}
    curr_task["name"]         = process
    curr_task["id"]           = task_id[process]
    curr_task["type"]         = "compute"
    curr_task["command"]      = script[process]                     #TODO: change to dictionary object?
    curr_task["parents"]      = parents[process]
    curr_task["children"]     = children[process]
    curr_task["files"]        = files[process]
    curr_task["runtime"]      = float(realtime[process])/1000.      #in seconds
    curr_task["avgCPU"]       = pct_cpu[process]
    curr_task["bytesRead"]    = float(rchar[process])/1000.         #in KB (not KiB!)
    curr_task["bytesWritten"] = float(wchar[process])/1000.         #in KB (not KiB!)
    curr_task["memory"]       = float(rss[process])/1000.           #in KB (not KiB!)

    tasks.append(curr_task)

workflow["tasks"] = tasks

#6. Write json to output file
outfile = "wfcommons-" + workflow_name + ".json"
with open(outfile, "w") as fp:
    fp.write(json.dumps(wfcommons, indent=4))




#Debug stuff
def print_process(process, task_id, realtime, pct_cpu, rss, rchar, wchar, read_bytes, write_bytes, work_dir, script, files, parents, children):
    print("\n=================================================================================\
=================================================================================\
\nProcess `{}`:".format(process))

    print("\ntask_id = {}".format(task_id[process]))
    #print("Requested CPUs = {}".format(cpus[process]))
    #print("Requested time = {} ms".format(time[process]))
    #print("Requested disk space = {} B".format(disk[process]))
    #print("Requested RAM space = {} B".format(memory[process]))

    print("\nResident set size = {} B".format(rss[process]))
    #print("Virtual memory size = {} B".format(vmem[process]))

    print("\nRealtime = {} ms".format(realtime[process]))
    print("% CPU = {}".format(pct_cpu[process]))
    #print("% memory = {} B".format(pct_mem[process]))

    print("\nTotal bytes read = {}".format(rchar[process]))
    print("Total bytes written = {}".format(wchar[process]))
    print("Number of bytes read from disk = {}".format(read_bytes[process]))
    print("Number of bytes written to disk = {}".format(write_bytes[process]))

    print("\nWork directory = {}".format(work_dir[process]))
    print("Script = \n{}".format(script[process]))

    print("\nFiles = {}".format(files[process]))

    print("\nParents = {}".format(parents[process]))
    print("\nChildren = {}".format(children[process]))

# for i, process in enumerate(processes):
#     #print("i = {}; process = {}".format(i, process))
#     print_process(process, task_id, realtime, pct_cpu, rss, rchar, wchar, read_bytes, write_bytes, work_dir, script, files, parents, children)
