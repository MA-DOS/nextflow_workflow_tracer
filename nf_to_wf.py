import os
import sys
import re
import networkx as nx
import json
from datetime import datetime, timezone
import subprocess

#Parse stdout
def parse_stdout(stdout, workflow):
    lines = stdout.split('\n')
    for line in lines:
        #print(f"{line=}")
        if "version" in line:
            x = line.split("version")[1].strip()
            print(f"Nextflow version:\n\t{x}")

        elif "Launching" in line:
            x = line.split("`")[1].strip()
            workflow["repo"] = x
            print(f"Workflow repo:\n\t{x}")

        elif "runName" in line:
            x = line[line.find(":")+1:].strip()
            workflow["runName"] = x
            print(f"runName:\n\t{x}")

        elif "Completed at" in line:
            x = line[line.find(":")+1:].strip() + " -1000"      #HST is -10 hours to UTC
            y = datetime.strptime(x, "%d-%b-%Y %H:%M:%S %z")

            #dunno if %d (zero-padded day of the month) or (%-d not zero-padded)
            #of if %H (zero-padded 24 hour clock hour) or (%-H not zero-padded)
            #%S (seconds) or (%-S)
            #print("executedAt:\n\t{}".format(x))
            #print("converted:\n\t{}".format(y.isoformat()))

            workflow["executedAt"] = str(y.isoformat())

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

            workflow["makespanInSeconds"] = seconds

#Parse scripts log
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

            #print(f"{curr_id=} {processes[curr_id]=} {realtime[curr_id]=} {pct_cpu[curr_id]=} {rss[curr_id]=} {rchar[curr_id]=} {wchar[curr_id]=} {read_bytes[curr_id]=} {write_bytes[curr_id]=} {work_dir[curr_id]=}")

#Parse filepath_log
def parse_log(filepath_log, task_id, processes, files, file_bytes_read, file_bytes_write):
    for i in task_id:
        process = processes[i]

        files[i]     = []
        remote_files = {}   #stores (path, index in files) for files that are not stored locally on the computer

        file_bytes_read [i] = 0.
        file_bytes_write[i] = 0.

        with open(filepath_log, "r") as fp:
            for count, line in enumerate(fp):
                if process in line:
                    if "Before run" in line:
                        if int(line[line.find('<') + 1 : line.find('>')].split(' ')[1]) == int(i):
                            #print(f"{line=}")
                            messages = line.split('-- messages:')[1].strip().split(',')
                            #print(f"{messages=}")

                            for item in messages:
                               temp = item.strip().replace("[", "").replace("]", "")

                               if ("." in temp or "/" in temp) and (":" not in temp):
                                    curr_file = {}
                                    curr_file["link"] = "input"

                                    x = temp.split("/")
                                    curr_file["name"] = x[len(x)-1]
                                    curr_file["path"] = "/".join(x[:len(x)-1]) + "/"

                                    if not os.path.exists(temp):
                                        curr_file["path"] = "/".join(x[:len(x)-1]) + "/"
                                        remote_files[temp] = len(files[i])        #input file pulled from nf-core test-datasets
                                        curr_file["sizeInBytes"] = 0
                                    else:
                                        curr_file["path"] = "/" + "/".join(x[len(x)-3:len(x)-1]) + "/"

                                        if os.path.isfile(temp):
                                            curr_file["sizeInBytes"] = os.path.getsize(temp)
                                            file_bytes_read[i] += curr_file["sizeInBytes"]
                                            
                                        elif os.path.isdir(temp):
                                            #print("{} is a directory".format(temp))
                                            total_size = 0
                                            for dpath, dname, fnames in os.walk(temp):
                                                for f in fnames:
                                                    x = os.path.join(dpath, f)

                                                    if os.path.isfile(x):
                                                        total_size += os.path.getsize(x)
                                                    elif os.path.islink(x):
                                                        total_size += os.path.getsize(os.readlink(x))

                                            curr_file["sizeInBytes"] = total_size
                                            file_bytes_read[i] += curr_file["sizeInBytes"]

                                        elif os.path.islink(temp):
                                            #print("{} is a symbolic link".format(temp))
                                            curr_file["sizeInBytes"] = os.path.getsize(os.readlink(temp))
                                            file_bytes_read[i] += curr_file["sizeInBytes"]

                                        else:
                                            print("{} exists! (but dunno what it is)".format(temp))

                                    files[i].append(curr_file)
                    elif "Binding out param:" in line:
                        if int(line.split(process)[1].split(' ')[1].strip()) == int(i):
                            x = line.split('=')[1].strip().split(',')

                            for item in x:
                                temp = item.strip().replace("[", "").replace("]", "")
                                if ("." in temp or "/" in temp) and (":" not in temp):
                                    curr_file = {}
                                    curr_file["link"] = "output"

                                    x = temp.split("/")
                                    curr_file["name"] = x[len(x)-1]

                                    curr_file["path"] = "/" + "/".join(x[len(x)-3:len(x)-1]) + "/"

                                    #note output files should always be written to tasks work dir (i.e., no remote files)
                                    if os.path.exists(temp):
                                        if os.path.isfile(temp):
                                            curr_file["sizeInBytes"] = os.path.getsize(temp)
                                            file_bytes_write[i] += curr_file["sizeInBytes"]
                                            
                                        elif os.path.isdir(temp):
                                            #print("{} is a directory".format(temp))
                                            total_size = 0
                                            for dpath, dname, fnames in os.walk(temp):
                                                for f in fnames:
                                                    x = os.path.join(dpath, f)

                                                    if os.path.isfile(x):
                                                        total_size += os.path.getsize(x)
                                                    elif os.path.islink(x):
                                                        total_size += os.path.getsize(os.readlink(x))

                                            curr_file["sizeInBytes"] = total_size
                                            file_bytes_write[i] += curr_file["sizeInBytes"]

                                        elif os.path.islink(temp):
                                            #print("{} is a symbolic link".format(temp))
                                            curr_file["sizeInBytes"] = os.path.getsize(os.readlink(temp))
                                            file_bytes_write[i] += curr_file["sizeInBytes"]

                                        else:
                                            print("{} exists! (but dunno what it is)".format(temp))

                                    files[i].append(curr_file)

        if len(remote_files) > 0:
            with open(filepath_log, "r") as fp:
                for count, line in enumerate(fp):
                    if "Copying foreign file" in line:
                        for j, rfile in enumerate(remote_files):
                            if rfile in line:
                                #print("Line {}: {}".format(count, line))
                                temp = line.split(rfile + " to work dir:")[1].strip()
                                #print("\tremote file {} ==> {}".format(rfile, temp))

                                if os.path.exists(temp):
                                    if os.path.isfile(temp):
                                        files[i][remote_files[rfile]]["sizeInBytes"] = os.path.getsize(temp)
                                        
                                        if files[i][remote_files[rfile]]["link"] == "input":
                                            file_bytes_read[i] += curr_file["sizeInBytes"]
                                        else:
                                            file_bytes_write[i] += curr_file["sizeInBytes"]

                                    elif os.path.isdir(temp):
                                        #print("{} is a directory".format(temp))
                                        total_size = 0
                                        for dpath, dname, fnames in os.walk(temp):
                                            for f in fnames:
                                                x = os.path.join(dpath, f)

                                                if os.path.isfile(x):
                                                    total_size += os.path.getsize(x)
                                                elif os.path.islink(x):
                                                    total_size += os.path.getsize(os.readlink(x))

                                        files[i][remote_files[rfile]]["sizeInBytes"] = total_size
                                        if files[i][remote_files[rfile]]["link"] == "input":
                                            file_bytes_read[i] += curr_file["sizeInBytes"]
                                        else:
                                            file_bytes_write[i] += curr_file["sizeInBytes"]

                                    elif os.path.islink(temp):
                                        #print("{} is a symbolic link".format(temp))
                                        files[i][remote_files[rfile]]["sizeInBytes"] = os.path.getsize(os.readlink(temp))
                                        if files[i][remote_files[rfile]]["link"] == "input":
                                            file_bytes_read[i] += curr_file["sizeInBytes"]
                                        else:
                                            file_bytes_write[i] += curr_file["sizeInBytes"]

                                    else:
                                        print("{} exists! (but dunno what it is)".format(temp))

        for f in files:
            f["name"] = f["path"].replace("/", "_") + "_" + f["name"]

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

##################################################################################################################

nextflow_path = "./nextflow-22.10.7/launch.sh"

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

workflow = {}
parse_stdout(completed_run.stdout, workflow)

if not os.path.isfile(filepath_trace):
    print(f"ERROR: trace file '{filepath_trace}' does not exist!")
    quit()
if not os.path.isfile(filepath_dag):
    print(f"ERROR: dag file '{filepath_dag}' does not exist!")
    quit()
if not os.path.isfile(filepath_log):
    print(f"ERROR: log file '{filepath_log}' does not exist!")
    quit()


#3. Check log for script commands ran
completed_log = subprocess.run([str(os.path.abspath(nextflow_path)), "log", workflow["runName"], "-t", "template-scriptlog.txt"], capture_output=True, encoding="utf-8")
#print(f"{completed_log.args} : {completed_log.stdout}")
scripts = {}
parse_scripts(completed_log.stdout, scripts)


#4. Parse filepath_trace
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


#5. Parse filepath_log
files            = {}
file_bytes_read  = {}
file_bytes_write = {}
parse_log(filepath_log, task_id, processes, files, file_bytes_read, file_bytes_write)


#6. Parse filepath_dag
parents  = {}
children = {}
parse_dag(filepath_dag, parents, children)


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

#ii. Create tasks
tasks = []
for i in task_id:
    curr_task                     = {}
    curr_task["name"]             = processes[i].replace(':', '.')
    curr_task["id"]               = i
    curr_task["type"]             = "compute"
    curr_task["command"]          = scripts[i]                       #TODO: change to dictionary object?
    curr_task["parents"]          = rep_parents[processes[i]] #parents[processes[i]]
    curr_task["children"]         = rep_children[processes[i]] #children[processes[i]]
    curr_task["files"]            = files[i]
    curr_task["runtimeInSeconds"] = float(realtime[i])/1000.0
    curr_task["avgCPU"]           = pct_cpu[i] #float(pct_cpu[i])
    curr_task["bytesRead"]        = int(rchar[i])
    curr_task["bytesWritten"]     = int(wchar[i])
    
    sum_r = 0.
    sum_w = 0.
    for file in files[i]:
        if isinstance(file["sizeInBytes"], str) == False:
            if file["link"] == "input":
                sum_r += file["sizeInBytes"]
            else:
                sum_w += file["sizeInBytes"]

    curr_task["inputFilesBytes"]  = sum_r 
    curr_task["outputFilesBytes"] = sum_w 

    curr_task["memoryInBytes"]           = rss[i]

    tasks.append(curr_task)

workflow["tasks"] = tasks

# Get machine info
workflow["machines"] = []
single_machine = {}

completed_process = subprocess.run(["uname", "-n"], capture_output=True, encoding="utf-8")
single_machine["nodeName"] = str(completed_process.stdout).strip()

completed_process = subprocess.run(["uname", "-s"], capture_output=True, encoding="utf-8")
single_machine["system"] = str(completed_process.stdout).strip()

completed_process = subprocess.run(["uname", "-r"], capture_output=True, encoding="utf-8")
single_machine["release"] = str(completed_process.stdout).strip()

completed_process = subprocess.run(["uname", "-m"], capture_output=True, encoding="utf-8")
single_machine["architecture"] = str(completed_process.stdout).strip()

cpu={}
cpu["count"] = 1  # Has to be 1, see (trace_nextflow.config)

# Get clock rate in 
Hz
command = "cat /proc/cpuinfo | grep 'model name' | sed 's/.* //' | sed 's/G.*//' | sed 's/\.//' | sed 's/$/0/'"
output = subprocess.check_output(command, shell=True)
output = output.decode('utf-8').strip()  # Convert bytes to string and remove trailing newline
cpu["speed"]=int(output)

single_machine["cpu"]=cpu
workflow["machines"].append(single_machine)


#8. Create top level structures for WfCommons JSON output
wfcommons                  = {}
wfcommons["name"]          = workflow_name
wfcommons["description"]   = "Trace generated from Nextflow (via https://github.com/wfcommons/nextflow_workflow_tracer)"
wfcommons["createdAt"]     = str(datetime.now(tz=timezone.utc).isoformat())
wfcommons["schemaVersion"] = "1.4"

wms         = {}
wms["name"] = "Nextflow"
wms["url"]  = "https://www.nextflow.io/"

wfcommons["wms"]      = wms
wfcommons["workflow"] = workflow


#4. Write JSON to output file
with open(outfile, "w") as fp:
    fp.write(json.dumps(wfcommons, indent=4))

