# nextflow_workflow_tracer
A tool to trace nextflow workflow executions and produce WfFormat workflow instances

## Modifications to Nextflow

We were unable to find a way to enable trace level logging in Nextflow, hence, small modifications to the source code were made in order to trace the inputs and outputs for each task in the workflow.
In particular, the file `TaskProcessor.groovy`, located in directory `/modules/nextflow/src/main/groovy/nextflow/processor/`, is modified. 
We reference the following version:
* https://github.com/nextflow-io/nextflow/blob/master/modules/nextflow/src/main/groovy/nextflow/processor/TaskProcessor.groovy
* commit: a5df62f6efffc24e0395ef05e9a43461174b453c

1. In function
```groovy
protected void bindOutputs0(Map<Short,List> tuples) {
    // -- bind out the collected values
    for( OutParam param : config.getOutputs() ) {
        final outValue = tuples[param.index]
        if( outValue == null )
            throw new IllegalStateException()

        if( outValue instanceof MissingParam ) {
            log.debug "Process $name > Skipping output binding because one or more optional files are missing: $outValue.missing"
            continue
        }

        log.trace "Process $name > Binding out param: ${param} = ${outValue}"
        bindOutParam(param, outValue)
    }
}
````
(starting on line 1415), modify line 1427
```groovy
log.trace "Process $name > Binding out param: ${param} = ${outValue}"
```
to output to `log.debug`
```groovy
log.debug "Process $name > Binding out param: ${param} = ${outValue}"
```

2. In function 
```groovy
@Override
Object messageArrived(final DataflowProcessor processor, final DataflowReadChannel<Object> channel, final int index, final Object message) {
    if( log.isTraceEnabled() ) {
        def channelName = config.getInputs()?.names?.get(index)
        def taskName = currentTask.get()?.name ?: name
        log.trace "<${taskName}> Message arrived -- ${channelName} => ${message}"
    }

    super.messageArrived(processor, channel, index, message)
}
```
(starting on line 2393), Remove the `if` statement on line 2394 (and its corresponding ending bracket on line 2398) and modify line 2397 
```groovy
log.trace "<${taskName}> Message arrived -- ${channelName} => ${message}"
```
to output to `log.debug`
```groovy
log.debug "<${taskName}> Message arrived -- ${channelName} => ${message}"
```
### Build the modified Nextflow

Follow the Nextflow build from source instructions, i.e., `make compile`, after which `./launch.sh` can be used while in the Nextflow root directory to start the modified Nextflow.


## Running a Nextflow workflow

The `parseNF.py` script requires several files in order to trace the execution of a Nextflow workflow. These files are generated via the following steps (we assume the `trace_nextflow.config` and `template-scriptlog.txt` files are in the Nextflow root directory, however, they can be located elsewhere if desired):

1. Run the workflow
```bash
./launch.sh -log <log file> run nf-core/<workflow> -profile test,docker -c trace_nextflow.config --outdir <workflow output directory> > <stdout file>
```
This will create a *log* file (via command line argument), a *trace* file (via `trace_nextflow.config`), a *dot* file (via `trace_nextflow.config`), and a *stdout* file (via bash command line).

2. Find the run name for the executed workflow
```bash
./launch.sh log
```
Alternatively, inspect the *stdout* file.

3. Get the script commands for each task in the workflow
```bash
./launch.sh log <run name> -t template-scriptlog.txt > <scripts log file>
```
This creates a *scripts* file (via `template-scriptlog.txt`).

### Notes

* We assume that on completion of the workflow (i.e., `workflow.onComplete`) the workflow prints the summary to stdout using `NfcoreTemplate.summary()`. To our knowledge, all nf-core workflows output this summary.

* Each task in the workflow requests a single CPU (via `trace_nextflow.config`).

* The *trace* and *dot* file locations are hardcoded in `trace_nextflow.config` and are automatically overwritten after each execution.


## Generating a WfFormat workflow instance

Workflow name and the 5 required files generated from the previous section are currently hardcoded in the `parseNF.py` script.

```bash
python3 parseNF.py
```
