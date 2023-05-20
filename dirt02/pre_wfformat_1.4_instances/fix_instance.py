#!/usr/bin/env python3

import sys
import json

with open(sys.argv[1]) as file:
       data = json.load(file)

# Fix Schema
data["schemaVersion"]="1.4"

# Fix version
data["wms"]["version"] = "23.04.1"

# Fix machines
machines = data["machines"]
machines[0]["system"]="linux"
machines[0]["cpu"]={"count":1,"speed":2400}
data["workflow"]["machines"] = machines
del data["machines"]

# Fix Makespan
data["workflow"]["makespanInSeconds"]=data["workflow"]["makespan"]
del data["workflow"]["makespan"]

# Fix data sizes (multiply by 1000)
for task in data["workflow"]["tasks"]:
    # Fix command
    command = task["command"]
    new_command = {}
    new_command["program"] = task["command"]
    new_command["arguments"] = []
    task["command"] = new_command
    task["memoryInBytes"] = int(float(task["memory"])*1000.0)
    del task["memory"]
    task["writtenBytes"] = int(float(task["bytesWritten"])*1000.0)
    del task["bytesWritten"]
    task["readBytes"] = int(float(task["bytesRead"])*1000.0)
    del task["bytesRead"]
    task["runtimeInSeconds"] = float(task["runtime"])
    del task["runtime"]
    task["inputFilesBytes"] = int(float(task["inputFilesBytes"])*1000.0)
    task["outputFilesBytes"] = int(float(task["outputFilesBytes"])*1000.0)


    # Fix files
    fixed_files = []
    for f in task["files"]:
        new_file = {}
        if f["size"] == "TODO - Remote file":
            continue
        new_file["link"] = f["link"]
        new_file["name"] = f["name"]
        new_file["path"] = f["path"]
        fixed_size = int(float(f["size"])*1000.0)
        new_file["sizeInBytes"] = fixed_size

        # Check for duplicate
        already_seen = False
        for s in fixed_files:
            if (s["name"] == new_file["name"]) and (s["path"] == new_file["path"]) and (s["link"] == new_file["link"]):
                already_seen = True
                break
        # If not a duplicate, add it
        if not already_seen:
            fixed_files.append(new_file)


    task["files"] = fixed_files

    


# Save it
output_file_name = sys.argv[1].split(".")[0] + "-dirt02-001.json"
with open("./fixed/"+output_file_name, 'w') as file:
      json.dump(data, file, indent=4)



